Successor of the so useful deferred library on Google AppEngine (GAE).

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
    def defer_one_task():
        yield deferred.defer_async(work, 'to be done')


    @ndb.tasklet
    def defer_multiple_tasks():
        yield deferred.defer_multi_async(
            deferred.task(work, 'Hello'),
            deferred.task(work, 'world!'))


    @ndb.tasklet
    def defer_deferring():
        # this one enqueues one task right now, which will then add all the
        # other tasks
        yield deferred.one_shot_async(
            deferred.task(work, 'Hello'),
            deferred.task(work, 'world!'))


You see, it pretty much looks and works as before.


Changes
=======

- `transactional`'s new default is `auto` (instead of `False`), t.i. if your
inside a transaction it will be set to True, otherwise it will be False
- You can set `_urlsuffix` which can be a str or a sequence of strs; which will
get appended to the url. Pure cosmetics.
- In case you set a `name` that is too long or contains invalid characters, a
hash of the name will be used instead.



