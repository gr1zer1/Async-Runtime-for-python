import socket
import threading
import time
import pytest
from async_runtime import EventLoop


def make_connected_pair():
    r, w = socket.socketpair()
    r.setblocking(False)
    w.setblocking(False)
    return r, w


def run_loop_once(loop: EventLoop):
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
        """Callback добавленный внутри callback не выполняется в той же итерации."""
        loop = EventLoop()
        order = []

        def first():
            order.append("first")
            loop.call_soon(order.append, "second")

        loop.call_soon(first)
        run_loop_once(loop)
        assert order == ["first"]

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
        assert called == []


# ---------------------------------------------------------------------------
# add_reader / remove_reader
# ---------------------------------------------------------------------------

class TestReader:
    def test_reader_fires_when_data_available(self):
        """Одна итерация: select видит данные → callback вызывается."""
        loop = EventLoop()
        r, w = make_connected_pair()
        called = []

        try:
            w.send(b"hello")
            loop.add_reader(r, called.append, "read")
            run_loop_once(loop)  # select + drain в одном _run_once
            assert "read" in called
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
        """Пока данные не вычитаны — reader срабатывает каждую итерацию."""
        loop = EventLoop()
        r, w = make_connected_pair()
        called = []

        try:
            w.send(b"x" * 10)
            loop.add_reader(r, called.append, "read")

            for _ in range(3):
                run_loop_once(loop)

            assert len(called) == 3
        finally:
            r.close()
            w.close()

    def test_reader_stops_after_data_consumed(self):
        """После того как данные вычитаны — reader больше не срабатывает."""
        loop = EventLoop()
        r, w = make_connected_pair()
        call_count = [0]

        def on_read():
            call_count[0] += 1
            r.recv(1024)  # вычитываем данные

        try:
            w.send(b"hello")
            loop.add_reader(r, on_read)
            run_loop_once(loop)  # срабатывает, вычитывает
            run_loop_once(loop)  # данных нет — не должен сработать
            assert call_count[0] == 1
        finally:
            r.close()
            w.close()


# ---------------------------------------------------------------------------
# add_writer / remove_writer
# ---------------------------------------------------------------------------

class TestWriter:
    def test_writer_fires_when_socket_writable(self):
        """Сокет почти всегда writable — callback должен сработать за одну итерацию."""
        loop = EventLoop()
        r, w = make_connected_pair()
        called = []

        try:
            loop.add_writer(w, called.append, "write")
            run_loop_once(loop)
            assert "write" in called
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
        """Пустой loop блокируется на select ~0.1s."""
        loop = EventLoop()
        start = time.monotonic()
        run_loop_once(loop)
        elapsed = time.monotonic() - start
        assert elapsed >= 0.05

    def test_ready_callbacks_use_zero_timeout(self):
        """Если есть callbacks — select с timeout=0, итерация быстрая."""
        loop = EventLoop()
        loop.call_soon(lambda: None)
        start = time.monotonic()
        run_loop_once(loop)
        elapsed = time.monotonic() - start
        assert elapsed < 0.05