#!/usr/bin/env python
#
# Copyright 2007 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
Patch of the original code:

- Bugfix: pass transactional when using the datastore for the pickled payload
- Bugfix: delete the entity if the task cannot be added
- New: defer_async(...)
- New: defer_multi_async(tasks) / defer_multi / defer_task
- New: Allow huge payloads using the blobstore
- New: Pass in `_urlsuffix` e.g. ('foo', 'bar') for easier readable weblogs.
         Task url will be set to "/_ah/queue/deferred/foo/bar"
- New: Pass in anything as `_name` and it will get sha256 hashed if its not a
        valid name already

"""


from google.appengine.ext import deferred as old_deferred


from collections import namedtuple, defaultdict
import hashlib


from google.appengine.api import taskqueue
from google.appengine.ext import ndb
from google.appengine.ext import blobstore
from google.appengine.api import files



_TASKQUEUE_HEADERS = {"Content-Type": "application/octet-stream"}
_DEFAULT_URL = "/_ah/queue/deferred"
_DEFAULT_QUEUE = "default"
_TRANSACTIONAL_DEFAULT = 'auto'
SMALL_PAYLOAD = 100000
LARGE_PAYLOAD = 1000000


PermanentTaskFailure = old_deferred.PermanentTaskFailure
run = old_deferred.run

class _DeferredTaskEntity(ndb.Model):
    """Datastore representation of a deferred task.

    This is used in cases when the deferred task is too big to be included as
    payload with the task queue entry.
    """
    _use_cache = False

    large = ndb.BlobProperty(indexed=False)
    huge = ndb.BlobKeyProperty(indexed=False)

    @classmethod
    def _get_kind(cls):
        return '_Deferred2_Payload'

    @property
    def payload(self):
        if self.large:
            return self.large
        elif self.huge:
            return blobstore.BlobReader(self.huge).read()

    @payload.setter
    def payload(self, value):
        if len(value) < LARGE_PAYLOAD:
            self.large = value
            return

        filename = files.blobstore.create(mime_type='application/octet-stream')
        with files.open(filename, 'a') as f:
            f.write(value)
        files.finalize(filename)

        self.huge = files.blobstore.get_blob_key(filename)

    @ndb.tasklet
    def delete_async(self):
        if self.huge:
            blobstore.BlobInfo(self.huge).delete()

        yield self.key.delete_async()

    def delete(self):
        return self.delete_async().get_result()


def run_from_datastore(key):
    """Retrieves a task from the datastore and executes it.

    Args:
        key: The datastore key of a _DeferredTaskEntity storing the task.
    Returns:
        The return value of the function invocation.
    """
    entity = key.get()
    if not entity:
        raise PermanentTaskFailure()
    try:
        run(entity.payload)
        entity.delete()
    except PermanentTaskFailure:
        entity.delete()
        raise


serialize = old_deferred.serialize


DeferredTask = namedtuple(
    'DeferredTask', ['queue_name', 'transactional', 'task_def'])  \
    # type: Tuple[str, bool, dict]

def convert_task_to_api_task(task_def):
    # type: (dict) -> taskqueue.Task
    return taskqueue.Task(**task_def)


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

    deferred = task(obj, *args, **kwargs)
    return enqueue_async(deferred)


def defer(obj, *args, **kwargs):
    # type: (...) -> taskqueue.Task
    return defer_async(obj, *args, **kwargs).get_result()


@ndb.tasklet
def enqueue_async(task):
    # type: (DeferredTask) -> Future[taskqueue.Task]
    (queue, transactional, task_def) = task


    if len(task_def['payload']) < SMALL_PAYLOAD:
        task = convert_task_to_api_task(task_def)
        rv = yield task.add_async(queue, transactional=transactional)
        raise ndb.Return(rv)
    else:
        entity = _DeferredTaskEntity()
        entity.payload = task_def['payload']
        yield entity.put_async()

        try:
            new_def = task_def.copy()
            new_def['payload'] = serialize(run_from_datastore, entity.key)
            task = convert_task_to_api_task(new_def)
            rv = yield task.add_async(queue, transactional=transactional)
        except Exception as e:
            if not ndb.in_transaction():
                yield entity.delete_async()

            raise e

        raise ndb.Return(rv)


@ndb.tasklet
def handle_big_payloads(task):
    # type: (DeferredTask) -> DeferredTask
    task_def = task.task_def

    if len(task_def['payload']) < SMALL_PAYLOAD:
        raise ndb.Return(task)
    else:
        if not ndb.in_transaction():
            raise taskqueue.BadTransactionState(
                'Handling BIG payloads touches the db and thus requires '
                'a transaction.')

        entity = _DeferredTaskEntity()
        entity.payload = task_def['payload']
        key = yield entity.put_async()

        new_def = task_def.copy()
        new_def['payload'] = serialize(run_from_datastore, key)
        new_task = task._replace(task_def=new_def)
        # new_task = DeferredTask(queue, transactional, new_def)
        raise ndb.Return(new_task)


@ndb.tasklet
def defer_multi_async(*tasks, **kwargs):
    # type: (List[DeferredTask]) -> Future[List[taskqueue.Task]]
    transform_fns = kwargs.pop(
        'transformers', [handle_big_payloads])  \
        # type: List[Callable[[DeferredTask], Optional[DeferredTask]]]
    if transform_fns:
        for fn in transform_fns:
            tasks = yield map(fn, tasks)
        tasks = filter(None, tasks)

    all_tasks = []
    grouped = defaultdict(list)
    for (queue, transactional, task) in tasks:
        api_task = convert_task_to_api_task(task)
        grouped[(queue, transactional)].append(api_task)
        all_tasks.append(api_task)


    yield [
        queue_multiple_tasks(queue, transactional, tasks)
        for (queue, transactional), tasks in grouped.iteritems()  # noqa: F812
    ]

    raise ndb.Return(all_tasks)

def defer_multi(*tasks):
    # type: (List[DeferredTask]) -> List[taskqueue.Task]
    return defer_multi_async(*tasks).get_result()


@ndb.tasklet
def queue_multiple_tasks(queue, transactional, tasks):
    # Wrapper b/c Queue().add_async returns a UserRPC but a MultiFuture only
    # accepts Futures as dependents.
    raise ndb.Return(
        (yield taskqueue.Queue(queue).add_async(tasks,
                                                transactional=transactional))
    )


def one_shot_async(*tasks):
    return defer_async(defer_multi, *tasks)


def one_shot(*tasks):
    return one_shot_async(*tasks).get_result()
