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

