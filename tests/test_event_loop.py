import socket
import threading
import time
import pytest
from async_runtime import EventLoop


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def make_connected_pair():
    """Возвращает (reader_sock, writer_sock) — неблокирующую пару сокетов."""
    r, w = socket.socketpair()
    r.setblocking(False)
    w.setblocking(False)
    return r, w


def run_loop_once(loop: EventLoop):
    """Одна итерация _run_once — удобно для unit-тестов."""
    loop._run_once()


# ---------------------------------------------------------------------------
# call_soon
# ---------------------------------------------------------------------------

class TestCallSoon:
    def test_callback_is_called(self):
        loop = EventLoop()
        called = []
        loop.call_soon(called.append, 1)
        run_loop_once(loop)
        assert called == [1]

    def test_multiple_callbacks_fifo_order(self):
        loop = EventLoop()
        order = []
        loop.call_soon(order.append, "a")
        loop.call_soon(order.append, "b")
        loop.call_soon(order.append, "c")
        run_loop_once(loop)
        assert order == ["a", "b", "c"]

    def test_callback_added_during_iteration_deferred(self):
        """Callback добавленный внутри callback не должен выполниться в той же итерации."""
        loop = EventLoop()
        order = []

        def first():
            order.append("first")
            loop.call_soon(order.append, "second")

        loop.call_soon(first)
        run_loop_once(loop)
        assert order == ["first"]          # "second" — в следующей итерации

        run_loop_once(loop)
        assert order == ["first", "second"]

    def test_callback_with_multiple_args(self):
        loop = EventLoop()
        received = []
        loop.call_soon(lambda a, b, c: received.extend([a, b, c]), 1, 2, 3)
        run_loop_once(loop)
        assert received == [1, 2, 3]

    def test_callback_not_called_before_run(self):
        loop = EventLoop()
        called = []
        loop.call_soon(called.append, 1)
        assert called == []  # ещё не запускали loop


# ---------------------------------------------------------------------------
# add_reader / remove_reader
# ---------------------------------------------------------------------------

class TestReader:
    def test_reader_fires_when_data_available(self):
        loop = EventLoop()
        r, w = make_connected_pair()
        called = []

        try:
            w.send(b"hello")
            loop.add_reader(r, called.append, "read")
            run_loop_once(loop)   # select видит readable fd
            run_loop_once(loop)   # drain _ready
            assert called == ["read"]
        finally:
            r.close()
            w.close()

    def test_reader_does_not_fire_without_data(self):
        loop = EventLoop()
        r, w = make_connected_pair()
        called = []

        try:
            loop.add_reader(r, called.append, "read")
            run_loop_once(loop)
            run_loop_once(loop)
            assert called == []
        finally:
            r.close()
            w.close()

    def test_remove_reader_stops_callbacks(self):
        loop = EventLoop()
        r, w = make_connected_pair()
        called = []

        try:
            w.send(b"data")
            loop.add_reader(r, called.append, "read")
            loop.remove_reader(r)
            run_loop_once(loop)
            run_loop_once(loop)
            assert called == []
        finally:
            r.close()
            w.close()

    def test_remove_reader_returns_true_if_existed(self):
        loop = EventLoop()
        r, w = make_connected_pair()
        try:
            loop.add_reader(r, lambda: None)
            assert loop.remove_reader(r) is True
        finally:
            r.close()
            w.close()

    def test_remove_reader_returns_false_if_not_existed(self):
        loop = EventLoop()
        r, w = make_connected_pair()
        try:
            assert loop.remove_reader(r) is False
        finally:
            r.close()
            w.close()

    def test_reader_fires_persistently(self):
        """add_reader персистентный — срабатывает каждую итерацию пока есть данные."""
        loop = EventLoop()
        r, w = make_connected_pair()
        called = []

        try:
            w.send(b"x" * 10)
            loop.add_reader(r, called.append, "read")

            for _ in range(3):
                run_loop_once(loop)  # select
                run_loop_once(loop)  # drain

            assert len(called) == 3
        finally:
            r.close()
            w.close()


# ---------------------------------------------------------------------------
# add_writer / remove_writer
# ---------------------------------------------------------------------------

class TestWriter:
    def test_writer_fires_when_socket_writable(self):
        loop = EventLoop()
        r, w = make_connected_pair()
        called = []

        try:
            loop.add_writer(w, called.append, "write")
            run_loop_once(loop)
            run_loop_once(loop)
            assert called == ["write"]
        finally:
            r.close()
            w.close()

    def test_remove_writer_stops_callbacks(self):
        loop = EventLoop()
        r, w = make_connected_pair()
        called = []

        try:
            loop.add_writer(w, called.append, "write")
            loop.remove_writer(w)
            run_loop_once(loop)
            run_loop_once(loop)
            assert called == []
        finally:
            r.close()
            w.close()

    def test_remove_writer_returns_true_if_existed(self):
        loop = EventLoop()
        r, w = make_connected_pair()
        try:
            loop.add_writer(w, lambda: None)
            assert loop.remove_writer(w) is True
        finally:
            r.close()
            w.close()

    def test_remove_writer_returns_false_if_not_existed(self):
        loop = EventLoop()
        r, w = make_connected_pair()
        try:
            assert loop.remove_writer(w) is False
        finally:
            r.close()
            w.close()


# ---------------------------------------------------------------------------
# run_forever / stop
# ---------------------------------------------------------------------------

class TestRunForever:
    def test_stop_halts_loop(self):
        loop = EventLoop()
        iterations = []

        def tick():
            iterations.append(1)
            if len(iterations) >= 3:
                loop.stop()
            else:
                loop.call_soon(tick)

        loop.call_soon(tick)
        loop.run_forever()
        assert len(iterations) == 3

    def test_run_forever_can_restart_after_stop(self):
        loop = EventLoop()
        called = []

        loop.call_soon(loop.stop)
        loop.run_forever()

        loop.call_soon(called.append, 1)
        loop.call_soon(loop.stop)
        loop.run_forever()

        assert called == [1]

    def test_stop_from_another_thread(self):
        loop = EventLoop()
        counter = []

        def counting_task():
            counter.append(1)
            loop.call_soon(counting_task)

        loop.call_soon(counting_task)

        t = threading.Thread(target=lambda: (time.sleep(0.05), loop.stop()))
        t.start()
        loop.run_forever()
        t.join()

        assert len(counter) > 0


# ---------------------------------------------------------------------------
# timeout behaviour
# ---------------------------------------------------------------------------

class TestTimeout:
    def test_no_busy_wait_when_idle(self):
        """Пустой loop должен блокироваться на select, а не крутиться вхолостую."""
        loop = EventLoop()
        start = time.monotonic()
        # запускаем одну итерацию без callbacks и без I/O — должна заблокироваться ~0.1s
        run_loop_once(loop)
        elapsed = time.monotonic() - start
        assert elapsed >= 0.05  # хотя бы половину таймаута подождал

    def test_ready_callbacks_use_zero_timeout(self):
        """Если есть callbacks — select с timeout=0, итерация быстрая."""
        loop = EventLoop()
        loop.call_soon(lambda: None)
        start = time.monotonic()
        run_loop_once(loop)
        elapsed = time.monotonic() - start
        assert elapsed < 0.05