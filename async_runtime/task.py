from future import Future, FutureStatus


class Task(Future):
    def __init__(self, event_loop, coro):
        super().__init__(event_loop)
        self._coro = coro
        self._event_loop.call_soon(self._step)
    

    def _step(self, exc=None):


        try:
            if exc is not None:
                result = self._coro.throw(exc)
            else:
                result = self._coro.send(None)
        except StopIteration as e:
            self.set_result(e.value)
        except Exception as e:
            self.set_exception(e)
        else:
            result.add_done_callback(self._wakeup)
    

    def _wakeup(self, future: Future):
        status = future._status
        if status == FutureStatus._PENDING:
            raise Exception
        
        exc = future.exception
        if exc is not None:
            self._step(exc)
        
        else:
            self._step()

