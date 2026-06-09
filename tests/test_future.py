import pytest
from async_runtime import EventLoop, Future, FutureStatus


def make_loop_and_future():
    loop = EventLoop()
    fut = Future(loop)
    return loop, fut


class TestInitialState:
    def test_initially_pending(self):
        _, fut = make_loop_and_future()
        assert not fut.done()
        assert not fut.cancelled()

    def test_result_raises_when_pending(self):
        _, fut = make_loop_and_future()
        with pytest.raises(ValueError, match="Future not done"):
            fut.result()

    def test_no_callbacks_initially(self):
        _, fut = make_loop_and_future()
        assert fut._callbacks == []


class TestSetResult:
    def test_done_after_set_result(self):
        _, fut = make_loop_and_future()
        fut.set_result(42)
        assert fut.done()

    def test_result_returns_value(self):
        _, fut = make_loop_and_future()
        fut.set_result(42)
        assert fut.result() == 42

    def test_result_none_is_valid(self):
        _, fut = make_loop_and_future()
        fut.set_result(None)
        assert fut.result() is None

    def test_result_false_is_valid(self):
        _, fut = make_loop_and_future()
        fut.set_result(False)
        assert fut.result() is False

    def test_set_result_twice_raises(self):
        _, fut = make_loop_and_future()
        fut.set_result(1)
        with pytest.raises(ValueError, match="already done"):
            fut.set_result(2)

    def test_set_result_on_cancelled_raises(self):
        _, fut = make_loop_and_future()
        fut.cancel()
        with pytest.raises(ValueError, match="already cancelled"):
            fut.set_result(1)


class TestSetException:
    def test_done_after_set_exception(self):
        _, fut = make_loop_and_future()
        fut.set_exception(ValueError("oops"))
        assert fut.done()

    def test_result_raises_stored_exception(self):
        _, fut = make_loop_and_future()
        exc = ValueError("oops")
        fut.set_exception(exc)
        with pytest.raises(ValueError, match="oops"):
            fut.result()

    def test_set_exception_twice_raises(self):
        _, fut = make_loop_and_future()
        fut.set_exception(ValueError("first"))
        with pytest.raises(ValueError, match="already done"):
            fut.set_exception(ValueError("second"))

    def test_set_exception_on_cancelled_raises(self):
        _, fut = make_loop_and_future()
        fut.cancel()
        with pytest.raises(ValueError, match="already cancelled"):
            fut.set_exception(RuntimeError("x"))


class TestCancel:
    def test_cancel_returns_true_when_pending(self):
        _, fut = make_loop_and_future()
        assert fut.cancel() is True

    def test_cancelled_after_cancel(self):
        _, fut = make_loop_and_future()
        fut.cancel()
        assert fut.cancelled()

    def test_done_returns_false_when_cancelled(self):
        _, fut = make_loop_and_future()
        fut.cancel()
        assert not fut.done()

    def test_cancel_returns_false_when_done(self):
        _, fut = make_loop_and_future()
        fut.set_result(1)
        assert fut.cancel() is False

    def test_cancel_returns_false_when_already_cancelled(self):
        _, fut = make_loop_and_future()
        fut.cancel()
        assert fut.cancel() is False

    def test_result_raises_when_cancelled(self):
        _, fut = make_loop_and_future()
        fut.cancel()
        with pytest.raises(ValueError, match="already cancelled"):
            fut.result()


class TestDoneCallback:
    def test_callback_called_on_set_result(self):
        loop, fut = make_loop_and_future()
        received = []
        fut.add_done_callback(received.append)
        fut.set_result(42)
        loop._run_once()
        assert received == [fut]

    def test_callback_called_on_set_exception(self):
        loop, fut = make_loop_and_future()
        received = []
        fut.add_done_callback(received.append)
        fut.set_exception(RuntimeError("fail"))
        loop._run_once()
        assert received == [fut]

    def test_multiple_callbacks_all_called(self):
        loop, fut = make_loop_and_future()
        calls = []
        fut.add_done_callback(lambda f: calls.append("a"))
        fut.add_done_callback(lambda f: calls.append("b"))
        fut.add_done_callback(lambda f: calls.append("c"))
        fut.set_result(1)
        loop._run_once()
        assert calls == ["a", "b", "c"]

    def test_callback_not_called_before_loop_runs(self):
        loop, fut = make_loop_and_future()
        called = []
        fut.add_done_callback(called.append)
        fut.set_result(99)
        assert called == []

    def test_callback_receives_future_as_argument(self):
        loop, fut = make_loop_and_future()
        received = []
        fut.add_done_callback(lambda f: received.append(f.result()))
        fut.set_result("hello")
        loop._run_once()
        assert received == ["hello"]


class TestAwait:
    def test_await_yields_self_when_pending(self):
        _, fut = make_loop_and_future()
        gen = fut.__await__()
        yielded = next(gen)
        assert yielded is fut

    def test_await_returns_result_when_done(self):
        _, fut = make_loop_and_future()
        fut.set_result(42)
        gen = fut.__await__()
        with pytest.raises(StopIteration) as exc_info:
            next(gen)
        assert exc_info.value.value == 42

    def test_await_raises_exception_when_set(self):
        _, fut = make_loop_and_future()
        fut.set_exception(ValueError("boom"))
        gen = fut.__await__()
        with pytest.raises(ValueError, match="boom"):
            next(gen)

    def test_await_resumes_after_set_result(self):
        _, fut = make_loop_and_future()
        gen = fut.__await__()
        next(gen)
        fut.set_result(7)
        with pytest.raises(StopIteration) as exc_info:
            gen.send(None)
        assert exc_info.value.value == 7