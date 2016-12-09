Intended as the next version of the so useful deferred library on Google AppEngine (GAE).

It should fix some issues.
It should enable async.



Usage
=====

By example::

    from google.appengine.ext import ndb
    import deferred2 as deferred

    def work(message):
        pass


    @ndb.tasklet
    def async_stuff():
        yield deferred.defer_async(work, 'to be done')


    @ndb.tasklet
    def multiple_tasks():
        yield deferred.defer_multi_async(
            deferred.task(work, i) for i in range(10))



Defaults::


    @deferred.defaults(queue='non_default')
    def work(message):
        pass


    defer(work, 'to be done')

    work = partial(defer, work, _queue='non_default')
    work('to be done')

