import pytest
uses = pytest.mark.usefixtures

from google.appengine.api import taskqueue

import deferred2 as deferred


messages = []

@pytest.fixture
def clear_messages():
    while messages:
        messages.pop()


def work(data):
    messages.append(data)

@uses('clear_messages', 'ndb')
class TestPayloadStores:
    def testSmall(self, deferreds):
        data = 'A'
        deferred.defer(work, data)

        assert deferred._DeferredTaskEntity.query().fetch() == []

        deferreds.consume()
        assert ['A'] == messages

    def testLarge(self, deferreds):
        data = 'A' * 100000
        deferred.defer(work, data)

        payload = deferred._DeferredTaskEntity.query().get()
        assert payload.large

        deferreds.consume()
        assert [data] == messages
        assert deferred._DeferredTaskEntity.query().fetch() == []

    def testHuge(self, deferreds, blobstore):
        data = 'A' * 1000000
        deferred.defer(work, data)

        payload = deferred._DeferredTaskEntity.query().get()
        assert payload.huge

        deferreds.consume()
        assert [data] == messages
        assert deferred._DeferredTaskEntity.query().fetch() == []
        assert blobstore.BlobInfo.all().fetch(limit=None) == []



DEFAULT_URL = deferred._DEFAULT_URL

@uses('clear_messages')
class TestAdditionalCosmeticUrlArguments:
    def testAddsArgsToTheUrl(self, deferreds):
        task = deferred.defer(work, 'A', _url_postfix='foo')
        assert task.url == DEFAULT_URL + "/foo"

        task = deferred.defer(work, 'A', _url_postfix=('foo'))
        assert task.url == DEFAULT_URL + "/foo"

        task = deferred.defer(work, 'A', _url_postfix=('foo',))
        assert task.url == DEFAULT_URL + "/foo"

        task = deferred.defer(work, 'A', _url_postfix=('foo','bar'))
        assert task.url == DEFAULT_URL + "/foo/bar"

    def testRemovesArgsBeforeCallingTheDeferred(self, deferreds):
        deferred.defer(work, 'A', _url_postfix=('foo','bar'))
        deferreds.consume()
        assert ['A'] == messages



def test_bug_transactional_used_with_large_payloads(ndb, deferreds):
    data = 'A' * 100000
    with pytest.raises(taskqueue.BadTransactionStateError):
        deferred.defer(work, data, _transactional=True)


def test_cleans_db_if_task_creation_fails(ndb, deferreds, monkeypatch):
    def fail(*a, **kw):
        raise taskqueue.Error()
    monkeypatch.setattr(taskqueue, 'Task', fail)

    data = 'A' * 100000
    with pytest.raises(taskqueue.Error):
        deferred.defer(work, data)

    assert deferred._DeferredTaskEntity.query().fetch() == []

def test_cleans_db_if_task_creation_fails_B(ndb, deferreds, monkeypatch):
    def fail(*a, **kw):
        raise taskqueue.Error()
    monkeypatch.setattr(taskqueue, 'Task', fail)

    data = 'A' * 100000
    with pytest.raises(taskqueue.Error):
        ndb.transaction(lambda: deferred.defer(work, data))

    assert deferred._DeferredTaskEntity.query().fetch() == []
