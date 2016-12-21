
from google.appengine.api import taskqueue
from google.appengine.ext import ndb

import deferred2 as deferred

import pytest
uses = pytest.mark.usefixtures


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
        deferred.defer(callable, data)

        assert deferred._DeferredTaskEntity.query().fetch() == []

        deferreds.consume()
        assert ['A'] == messages


    def testLarge(self, deferreds, callable):
        data = 'A' * 100000
        deferred.defer(callable, data)

        payload = deferred._DeferredTaskEntity.query().get()
        assert payload.large

        deferreds.consume()
        assert [data] == messages
        assert deferred._DeferredTaskEntity.query().fetch() == []


    def testHuge(self, deferreds, blobstore, callable):
        data = 'A' * 1000000
        deferred.defer(callable, data)

        payload = deferred._DeferredTaskEntity.query().get()
        assert payload.huge

        deferreds.consume()
        assert [data] == messages
        assert deferred._DeferredTaskEntity.query().fetch() == []
        assert blobstore.BlobInfo.all().fetch(limit=None) == []


@uses('clear_messages', 'ndb')
class TestNameMungling:

    def testHashifyTooLongNames(self, deferreds):
        name = 'N' * 1000
        deferred.defer(work, _name=name)


    def testHashifyNonStrings(self, deferreds):
        name = ndb.Key('User', '1234567', 'Order', 'as2897')
        deferred.defer(work, _name=name)


DEFAULT_URL = deferred._DEFAULT_URL

@uses('clear_messages')
class TestAdditionalCosmeticUrlArguments:
    def testAddsArgsToTheUrl(self, deferreds):
        task = deferred.defer(work, 'A', _urlsuffix='foo')
        assert task.url == DEFAULT_URL + "/foo"

        task = deferred.defer(work, 'A', _urlsuffix=('foo'))
        assert task.url == DEFAULT_URL + "/foo"

        task = deferred.defer(work, 'A',
                              _urlsuffix=('foo', ndb.Key('User', '1234').id()))
        assert task.url == DEFAULT_URL + "/foo/1234"

        task = deferred.defer(work, 'A', _urlsuffix=('foo', 'bar'))
        assert task.url == DEFAULT_URL + "/foo/bar"

    def testRemovesArgsBeforeCallingTheDeferred(self, deferreds):
        deferred.defer(work, 'A', _urlsuffix=('foo', 'bar'))
        deferreds.consume()
        assert ['A'] == messages


    def testAutoName(self, taskqueue):
        deferred.defer(work, 'A')

        print work.__module__, work.__name__
        print work.__class__
        inst = _Work()
        meth = inst.append_message
        print inst.__class__
        print inst.__class__.__module__, inst.__class__.__name__
        print meth.__name__
        # 1/0



class TestAutoTransactional:

    @pytest.fixture
    def taskMock(self, mockito, ndb):
        resolvedFuture = ndb.Future()
        resolvedFuture.set_result(None)

        taskMock = mockito.mock(strict=True)
        mockito.when(taskMock).add_async(
            mockito.any(str),
            transactional=mockito.any(bool)).thenReturn(resolvedFuture)

        mockito.when(taskqueue).Task(
            url=mockito.any(str),
            headers=mockito.any(dict),
            payload=mockito.any(str)).thenReturn(taskMock)

        yield taskMock

        mockito.verifyNoMoreInteractions(taskMock)


    def testTransactionalIfInTransaction(self, mockito, ndb, taskMock):

        ndb.transaction(lambda: deferred.defer(work, 'A'))
        mockito.verify(taskMock).add_async('default', transactional=True)


    def testNotTransactionalIfOutsideTransaction(
            self, mockito, ndb, taskMock):

        deferred.defer(work, 'A')
        mockito.verify(taskMock).add_async('default', transactional=False)


    def testNotTransactionalIfWanted(self, mockito, ndb, taskMock):

        ndb.transaction(
            lambda: deferred.defer(work, 'A', _transactional=False))
        mockito.verify(taskMock).add_async('default', transactional=False)


    def testCannotOptinToTransactionalOutsideOfTransaction(self, mockito, ndb):

        with pytest.raises(taskqueue.BadTransactionStateError):
            deferred.defer(work, 'A', _transactional=True)


    def testTransactionalMustBeFalseIfNameIsGiven(self, taskqueue, ndb):
        ndb.transaction(lambda: deferred.defer(work, 'A', _name='a'))





@uses('clear_messages', 'ndb')
class TestCleanDbIfAddingTheTaskFails:
    def testTransactionalSpecifiedButNotInTransaction(self, deferreds):
        data = 'A' * 100000
        with pytest.raises(taskqueue.BadTransactionStateError):
            deferred.defer(work, data, _transactional=True)

        assert deferred._DeferredTaskEntity.query().fetch() == []

    def testTaskCreationFailsNonTransactional(self, deferreds, monkeypatch):
        def fail(*a, **kw):
            raise taskqueue.Error()
        monkeypatch.setattr(taskqueue, 'Task', fail)

        data = 'A' * 100000
        with pytest.raises(taskqueue.Error):
            deferred.defer(work, data)

        assert deferred._DeferredTaskEntity.query().fetch() == []

    def testTaskCreationFailsInTransaction(self, deferreds, monkeypatch):
        def fail(*a, **kw):
            raise taskqueue.Error()
        monkeypatch.setattr(taskqueue, 'Task', fail)

        data = 'A' * 100000
        with pytest.raises(taskqueue.Error):
            ndb.transaction(lambda: deferred.defer(work, data))

        assert deferred._DeferredTaskEntity.query().fetch() == []


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

    def testBigPayloadsRequireTransaction(self, deferreds):
        with pytest.raises(taskqueue.BadTransactionStateError):
            deferred.defer_multi(
                *[deferred.task(work, str(i) * 100000) for i in range(5)])




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





