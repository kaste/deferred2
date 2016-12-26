
from google.appengine.api import taskqueue
from google.appengine.ext import ndb

import deferred2 as deferred
from deferred2 import defer, defer_multi, task

import pytest
uses = pytest.mark.usefixtures


BigPayloadEntity = deferred.ext.big_payloads.BigPayload


def ResolvedFuture(val):
    fut = ndb.Future()
    fut.set_result(val)
    return fut


FutureNone = ResolvedFuture(None)


@pytest.fixture
def mockito():
    import mockito

    yield mockito

    mockito.unstub()


messages = []

@pytest.fixture
def clear_messages():
    messages[:] = []


def work(data):
    messages.append(data)


class _Work(object):
    def append_message(self, data):
        messages.append(data)

@pytest.fixture(ids=['function', 'method'],
                params=[lambda: work, lambda: _Work().append_message])
def callable(request, clear_messages):
    yield request.param()


@uses('clear_messages', 'ndb')
class TestPayloadStores:
    def testSmall(self, deferreds, callable):
        data = 'A'
        defer(callable, data)

        assert BigPayloadEntity.query().fetch() == []

        deferreds.consume()
        assert ['A'] == messages


    def testLarge(self, deferreds, callable):
        data = 'A' * 100000
        defer(callable, data)

        payload = BigPayloadEntity.query().get()
        assert payload.large

        deferreds.consume()
        assert [data] == messages
        assert BigPayloadEntity.query().fetch() == []


    def testHuge(self, deferreds, blobstore, callable):
        data = 'A' * 1000000
        defer(callable, data)

        payload = BigPayloadEntity.query().get()
        assert payload.huge

        deferreds.consume()
        assert [data] == messages
        assert BigPayloadEntity.query().fetch() == []
        assert blobstore.BlobInfo.all().fetch(limit=None) == []


@uses('clear_messages', 'ndb')
class TestNameMungling:

    def testHashifyTooLongNames(self, deferreds):
        name = 'N' * 1000
        defer(work, _name=name)


    def testHashifyNonStrings(self, deferreds):
        name = ndb.Key('User', '1234567', 'Order', 'as2897')
        defer(work, _name=name)


DEFAULT_URL = deferred._DEFAULT_URL

@uses('clear_messages')
class TestAdditionalCosmeticUrlArguments:
    def testAddsArgsToTheUrl(self, deferreds):
        task = defer(work, 'A', _urlsuffix='foo')
        assert task.url == DEFAULT_URL + "/foo"

        task = defer(work, 'A', _urlsuffix=('foo'))
        assert task.url == DEFAULT_URL + "/foo"

        task = defer(work, 'A',
                              _urlsuffix=('foo', ndb.Key('User', '1234').id()))
        assert task.url == DEFAULT_URL + "/foo/1234"

        task = defer(work, 'A', _urlsuffix=('foo', 'bar'))
        assert task.url == DEFAULT_URL + "/foo/bar"

    def testRemovesArgsBeforeCallingTheDeferred(self, deferreds):
        defer(work, 'A', _urlsuffix=('foo', 'bar'))
        deferreds.consume()
        assert ['A'] == messages


    def testAutoName(self, taskqueue):
        defer(work, 'A')

        print work.__module__, work.__name__
        print work.__class__
        inst = _Work()
        meth = inst.append_message
        print inst.__class__
        print inst.__class__.__module__, inst.__class__.__name__
        print meth.__name__
        # 1/0



class TestAutoTransactional:

    def testTransactionalIfInTransaction(self, mockito, ndb):

        mockito.when(deferred.batcher) \
            .queue_multiple_tasks('default', True, mockito.any(list)) \
            .thenReturn(FutureNone)

        ndb.transaction(lambda: defer(work, 'A'))


    def testNotTransactionalIfOutsideTransaction(self, mockito, ndb):

        mockito.when(deferred.batcher) \
            .queue_multiple_tasks('default', False, mockito.any(list)) \
            .thenReturn(FutureNone)

        defer(work, 'A')


    def testNotTransactionalIfWanted(self, mockito, ndb):

        mockito.when(deferred.batcher) \
            .queue_multiple_tasks('default', False, mockito.any(list)) \
            .thenReturn(FutureNone)

        ndb.transaction(
            lambda: defer(work, 'A', _transactional=False))


    def testCannotOptinToTransactionalOutsideOfTransaction(self, mockito, ndb):

        with pytest.raises(taskqueue.BadTransactionStateError):
            defer(work, 'A', _transactional=True)


    def testTransactionalMustBeFalseIfNameIsGiven(self, taskqueue, ndb):
        ndb.transaction(lambda: defer(work, 'A', _name='a'))





@uses('clear_messages', 'ndb')
class TestCleanDbIfAddingTheTaskFails:
    def testTransactionalSpecifiedButNotInTransaction(self, deferreds):
        data = 'A' * 100000
        with pytest.raises(taskqueue.BadTransactionStateError):
            defer(work, data, _transactional=True)

        assert BigPayloadEntity.query().fetch() == []

    def testTaskCreationFailsNonTransactional(self, deferreds, monkeypatch):
        def fail(*a, **kw):
            raise taskqueue.Error()
        monkeypatch.setattr(taskqueue, 'Task', fail)

        data = 'A' * 100000
        with pytest.raises(taskqueue.Error):
            defer(work, data)

        assert BigPayloadEntity.query().fetch() == []

    def testTaskCreationFailsInTransaction(self, deferreds, monkeypatch):
        def fail(*a, **kw):
            raise taskqueue.Error()
        monkeypatch.setattr(taskqueue, 'Task', fail)

        data = 'A' * 100000
        with pytest.raises(taskqueue.Error):
            ndb.transaction(lambda: defer(work, data))

        assert BigPayloadEntity.query().fetch() == []


@uses('clear_messages', 'ndb')
class TestAddMultipleTasks:

    def testAsync(self, deferreds):

        deferred.defer_multi_async(
            *[deferred.task(work, i) for i in range(10)]).get_result()

        deferreds.consume()

        assert sorted(messages) == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]


    def testSync(self, deferreds):

        deferred.defer_multi(*[deferred.task(work, i) for i in range(10)])

        deferreds.consume()

        assert sorted(messages) == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]


    def testFilterWhenTranformedIntoNone(self, deferreds):
        tasks = deferred.defer_multi(
            deferred.task(work, 'A'),
            transformers=[ndb.tasklet(lambda t, c: None)])

        assert len(tasks) == 0


    class TestBatching:
        def testDifferentiateTransactional(self, mockito):

            def assertTasksLen(wanted_length):
                def _answer(q, tr, tasks):
                    assert len(tasks) == wanted_length
                    return FutureNone
                return _answer

            mockito.when(deferred.batcher) \
                .queue_multiple_tasks('Foo', False, mockito.any(list)) \
                .thenAnswer(assertTasksLen(2))
            mockito.when(deferred.batcher) \
                .queue_multiple_tasks('Foo', True, mockito.any(list)) \
                .thenAnswer(assertTasksLen(1))

            defer_multi(
                task(work, 'A', _queue='Foo'),
                task(work, 'B', _queue='Foo'),
                task(work, 'A', _queue='Foo', _transactional=True))


        def testDifferentiateQueueName(self, mockito):

            def assertTasksLen(wanted_length):
                def _answer(q, tr, tasks):
                    assert len(tasks) == wanted_length
                    return FutureNone
                return _answer

            mockito.when(deferred.batcher) \
                .queue_multiple_tasks('Foo', False, mockito.any(list)) \
                .thenAnswer(assertTasksLen(2))
            mockito.when(deferred.batcher) \
                .queue_multiple_tasks('Bar', False, mockito.any(list)) \
                .thenAnswer(assertTasksLen(1))

            defer_multi(
                task(work, 'A', _queue='Foo'),
                task(work, 'B', _queue='Foo'),
                task(work, 'A', _queue='Bar'))




@uses('clear_messages', 'ndb')
class TestOneShot:
    def testEnqueueOneTaskWhichEnqueuesTheRest(self, deferreds):

        deferred.one_shot(
            *[deferred.task(work, i) for i in range(10)]
        )
        assert deferreds.count_tasks() == 1
        deferreds.tick()
        assert deferreds.count_tasks() == 10
        deferreds.consume()





