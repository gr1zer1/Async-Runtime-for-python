import select
from collections import deque
from typing import Callable


class EventLoop:
    def __init__(self):
        self._ready = deque()
        self._readers = {}
        self._writers = {}
        self._stopped = False

    
    def call_soon(self, callback: Callable, *args):
        self._ready.append(
            (
                callback,
                args
            )
        )

    def add_reader(self, fd, callback: Callable, *args):
        self._readers[fd] = (callback, args)


    def remove_reader(self, fd):
        return self._readers.pop(fd, None) is not None
    

    def add_writer(self, fd, callback: Callable, *args):
        self._writers[fd] = (callback, args)


    def remove_writer(self, fd):
        return self._writers.pop(fd, None) is not None
    

    def _process_events(self, events: list, coll: dict):
        for fd in events:
            if fd in coll:
                callable, args = coll[fd]
                self.call_soon(callable, *args)

    def _run_once(self):
        

        timeout = 0 if self._ready else 0.1

        r_fds = list(self._readers.keys())
        w_fds = list(self._writers.keys())


        try:

            readable,writeable,_ = select.select(r_fds, w_fds, [], timeout)

        except OSError:
            return
        

        self._process_events(readable, self._readers)
        self._process_events(writeable, self._writers)

        ntasks = len(self._ready)

        for _ in range(ntasks):
            if self._stopped:
                break
            callable, args = self._ready.popleft()
            callable(*args)


    def run_forever(self):
        self._stopped = False

        while not self._stopped:
            self._run_once()

    def stop(self):
        self._stopped = True
