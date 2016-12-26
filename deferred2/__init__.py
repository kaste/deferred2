# @license
# Copyright (c) 2016 herr.kaste <herr.kaste@gmail.com>. All rights reserved.
# This code may only be used under the MIT style license.


"""
Patch of the original code:

- Bugfix: pass transactional when using the datastore for the pickled payload
- Bugfix: delete the entity if the task cannot be added
- New: defer_async(...)
- New: defer_multi_async(tasks) / defer_multi
- New: Allow huge payloads using the blobstore
- New: Pass in `_urlsuffix` e.g. ('foo', 'bar') for easier readable weblogs.
         Task url will be set to "/_ah/queue/deferred/foo/bar"
- New: Pass in anything as `_name` and it will get sha256 hashed if its not a
        valid name already

"""


from google.appengine.api import taskqueue
from google.appengine.ext import ndb
from google.appengine.ext import deferred as old_deferred


from collections import namedtuple
import itertools as it
import hashlib

from . import ext
from . import batcher



_TASKQUEUE_HEADERS = {"Content-Type": "application/octet-stream"}
_DEFAULT_URL = "/_ah/queue/deferred"
_DEFAULT_QUEUE = "default"
_TRANSACTIONAL_DEFAULT = 'auto'
_DEFAULT_EXTENSIONS = [ext.handle_big_payloads]

serialize = old_deferred.serialize


DeferredTask = namedtuple(
    'DeferredTask', ['queue_name', 'transactional', 'task_def'])  \
    # type: Tuple[str, bool, dict]


def task(obj, *args, **kwargs):
    # type: (...) -> DeferredTask
    taskargs = {x[1:]: kwargs.pop(x)
                for x in ("_countdown", "_eta", "_name", "_target",
                          "_url", "_retry_options") if x in kwargs}

    taskargs.setdefault('url', _DEFAULT_URL)
    taskargs["headers"] = dict(_TASKQUEUE_HEADERS)
    taskargs["headers"].update(kwargs.pop("_headers", {}))

    urlsuffix = kwargs.pop("_urlsuffix", None)
    if urlsuffix:
        if not isinstance(urlsuffix, basestring):
            urlsuffix = "/".join(map(str, urlsuffix))
        taskargs["url"] += "/{}".format(urlsuffix)

    if 'name' in taskargs:
        name = taskargs['name']
        if not isinstance(name, basestring):
            name = str(name)

        if not taskqueue.taskqueue._TASK_NAME_RE.match(name):
            name = hashlib.sha256(name).hexdigest()

        taskargs['name'] = name


    transactional = kwargs.pop("_transactional", _TRANSACTIONAL_DEFAULT)
    if transactional == 'auto':
        transactional = False if 'name' in taskargs else ndb.in_transaction()
    queue = kwargs.pop("_queue", _DEFAULT_QUEUE)

    taskargs['payload'] = serialize(obj, *args, **kwargs)

    return DeferredTask(queue_name=queue,
                        transactional=transactional,
                        task_def=taskargs)


@ndb.tasklet
def defer_async(obj, *args, **kwargs):
    # type: (...) -> Future[taskqueue.Task]
    """Defers a callable for execution later.

    The default deferred URL of /_ah/queue/deferred will be used unless an
    alternate URL is explicitly specified. If you want to use the default URL
    for a queue, specify _url=None. If you specify a different URL, you will
    need to install the handler on that URL (see the module docstring for
    details).

    Args:
        obj: The callable to execute. See module docstring for restrictions.
        args: Positional arguments to call the callable with.
        kwargs: _countdown, _eta, _headers, _name, _target, _transactional,
                _url, _retry_options, _queue: Passed through to the task queue
                - see the task queue documentation for details.
                Any other keyword arguments are passed through to the callable.
    Returns:
        A taskqueue.Task object which represents an enqueued callable.
    """

    tasks = yield defer_multi_async(task(obj, *args, **kwargs))
    raise ndb.Return(tasks[0] if tasks else None)


def defer(obj, *args, **kwargs):
    # type: (...) -> taskqueue.Task
    return defer_async(obj, *args, **kwargs).get_result()



@ndb.tasklet
def final_transformation(task, on_rollback):
    (queue, transactional, task_def) = task
    return (queue, transactional, taskqueue.Task(**task_def))


def flatten(iterable):
    return list(it.chain.from_iterable(iterable))

def unzip(iterable):
    return zip(*iterable)


class RollbackManager(object):
    def __init__(self, size):
        self._stacks = [CallbackManager() for _ in xrange(size)]

    @ndb.tasklet
    def close_async(self):
        yield [s.close_async() for s in self._stacks]

    def __len__(self):
        return len(self._stacks)

    def __iter__(self):
        return iter(self._stacks)



class CallbackManager(object):
    def __init__(self):
        self._callbacks = []

    def add(self, fn):
        self._callbacks.append(fn)

    @ndb.tasklet
    def close_async(self):
        yield [c() for c in self.pop_all()]

    def pop_all(self):
        rv = self._callbacks[:]
        self._callbacks = []
        return rv


def _make_apply_fn(fn):
    @ndb.tasklet
    def applier(t, s):
        t = yield fn(t, s.add)
        raise ndb.Return((t, s))
    return applier


@ndb.tasklet
def defer_multi_async(*tasks, **kwargs):
    # type: (List[DeferredTask]) -> Future[List[taskqueue.Task]]

    transformers = kwargs.pop(
        'transformers', _DEFAULT_EXTENSIONS)  \
        # type: List[Callable[[DeferredTask], Optional[DeferredTask]]]

    rollback_stacks = RollbackManager(len(tasks))

    try:
        tasks = yield _apply_and_enqueue(transformers, tasks, rollback_stacks)
    finally:
        yield rollback_stacks.close_async()

    raise ndb.Return(tasks)


@ndb.tasklet
def _apply_and_enqueue(transformers, tasks, stacks):
    transformers = transformers + [final_transformation]
    # We need to transform the transformers bc we want the parallel yield below
    transformers = map(_make_apply_fn, transformers)

    data = zip(tasks, stacks)
    # We generally expect and filter None's. But bc the last transformation
    # never returns a None, we're done after this loop and don't have to filter
    # once again.
    for fn in transformers:
        data = yield [fn(t, s) for (t, s) in data if t]

    if not data:
        raise ndb.Return([])

    tasks = unzip(data)[0]
    try:
        tasks = yield batcher.batch_enqueue_tasks_async(tasks)
    finally:
        # Inverse logic: For all the successful tasks we pop the registered
        # callbacks. In the outer scope we unconditionally rollback everything
        # else. This is to ensure that all tasks we lost during transformation
        # (the transformator returned None or raised), get rolled back.
        for (_, _, task), stack in data:
            if task.was_enqueued:
                stack.pop_all()

    raise ndb.Return(tasks)


def defer_multi(*tasks, **kwargs):
    # type: (List[DeferredTask]) -> List[taskqueue.Task]
    return defer_multi_async(*tasks, **kwargs).get_result()


def one_shot_async(*tasks):
    return defer_async(defer_multi, *tasks)


def one_shot(*tasks):
    return one_shot_async(*tasks).get_result()
