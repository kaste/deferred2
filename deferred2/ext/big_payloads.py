
from google.appengine.ext import ndb
from google.appengine.ext import blobstore
from google.appengine.api import files

from google.appengine.ext import deferred as old_deferred


SMALL_PAYLOAD = 100000
LARGE_PAYLOAD = 1000000

PermanentTaskFailure = old_deferred.PermanentTaskFailure
run = old_deferred.run
serialize = old_deferred.serialize


class BigPayload(ndb.Model):
    """Datastore representation of a deferred task.

    This is used in cases when the deferred task is too big to be included as
    payload with the task queue entry.
    """
    _use_cache = False

    large = ndb.BlobProperty(indexed=False)
    huge = ndb.BlobKeyProperty(indexed=False)

    @classmethod
    def _get_kind(cls):
        return '_Df2_Payload'

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
        key: The datastore key of a BigPayload storing the task.
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


@ndb.tasklet
def handle_big_payloads(task, on_rollback):
    # type: (DeferredTask, Callable) -> Future[DeferredTask]

    task_def = task.task_def

    if len(task_def['payload']) < SMALL_PAYLOAD:
        raise ndb.Return(task)
    else:
        entity = BigPayload()
        entity.payload = task_def['payload']
        key = yield entity.put_async()
        if entity.huge or not ndb.in_transaction():
            on_rollback(lambda: entity.delete_async())

        new_def = task_def.copy()
        new_def['payload'] = serialize(run_from_datastore, key)
        new_task = task._replace(task_def=new_def)
        raise ndb.Return(new_task)

