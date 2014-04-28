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
- New: Allow huge payloads using the blobstore
- New: Pass in _url_postfix used as e.g. ('foo', 'bar') => "/_ah/queue/deferred/foo/bar"

"""


from google.appengine.ext import deferred as old_deferred


import logging

from google.appengine.api import taskqueue
from google.appengine.ext import ndb
from google.appengine.ext import blobstore
from google.appengine.api import files



_TASKQUEUE_HEADERS = {"Content-Type": "application/octet-stream"}
_DEFAULT_URL = "/_ah/queue/deferred"
_DEFAULT_QUEUE = "default"
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

    def delete(self):
        if self.huge:
            blobstore.BlobInfo(self.huge).delete()

        self.key.delete()


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

def defer(obj, *args, **kwargs):
    """Defers a callable for execution later.

    The default deferred URL of /_ah/queue/deferred will be used unless an
    alternate URL is explicitly specified. If you want to use the default URL for
    a queue, specify _url=None. If you specify a different URL, you will need to
    install the handler on that URL (see the module docstring for details).

    Args:
        obj: The callable to execute. See module docstring for restrictions.
                _countdown, _eta, _headers, _name, _target, _transactional, _url,
                _retry_options, _queue: Passed through to the task queue - see the
                task queue documentation for details.
        args: Positional arguments to call the callable with.
        kwargs: Any other keyword arguments are passed through to the callable.
    Returns:
        A taskqueue.Task object which represents an enqueued callable.
    """
    taskargs = dict((x, kwargs.pop(("_%s" % x), None))
                            for x in ("countdown", "eta", "name", "target",
                                                "retry_options"))
    taskargs["url"] = kwargs.pop("_url", _DEFAULT_URL)
    url_postfix = kwargs.pop("_url_postfix", None)
    if url_postfix:
        if isinstance(url_postfix, basestring):
            url_postfix = [url_postfix]
        taskargs["url"] += "/%s" % "/".join(url_postfix)
    transactional = kwargs.pop("_transactional", False)
    taskargs["headers"] = dict(_TASKQUEUE_HEADERS)
    taskargs["headers"].update(kwargs.pop("_headers", {}))
    queue = kwargs.pop("_queue", _DEFAULT_QUEUE)

    pickled = serialize(obj, *args, **kwargs)
    if len(pickled) < SMALL_PAYLOAD:
        task = taskqueue.Task(payload=pickled, **taskargs)
        return task.add(queue, transactional=transactional)
    else:
        entity = _DeferredTaskEntity()
        entity.payload = pickled
        key = entity.put()

        pickled = serialize(run_from_datastore, key)
        try:
            task = taskqueue.Task(payload=pickled, **taskargs)
            return task.add(queue, transactional=transactional)
        except taskqueue.Error:
            if not ndb.in_transaction():
                entity.delete()
            raise

