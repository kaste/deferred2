
from google.appengine.api import taskqueue
from google.appengine.ext import ndb

from collections import defaultdict


# A task herein is a Tuple[str, bool, taskqueue.Task]


class Batcher(object):
    def __init__(self):
        self._grouped = defaultdict(list)
        self._tasks = []

    def add(self, task):
        (queue, transactional, task) = task
        self._grouped[(queue, transactional)].append(task)
        self._tasks.append(task)

    @ndb.tasklet
    def run_async(self):
        # type: () -> Future[List[taskqueue.Task]]
        yield [
            queue_multiple_tasks(queue, transactional, tasks)
            for (queue, transactional), tasks in self._grouped.iteritems()
        ]
        raise ndb.Return(self._tasks)


@ndb.tasklet
def queue_multiple_tasks(queue, transactional, tasks):
    # Wrapper b/c Queue().add_async returns a UserRPC but a MultiFuture only
    # accepts Futures as dependents.
    raise ndb.Return(
        (yield taskqueue.Queue(queue).add_async(tasks,
                                                transactional=transactional))
    )


def batch_enqueue_tasks_async(tasks):
    """Convenience function to enqueue multiple tasks"""
    batcher = Batcher()
    map(batcher.add, tasks)
    return batcher.run_async()

