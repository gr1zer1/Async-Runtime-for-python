from enum import Enum
from async_runtime import EventLoop
from typing import Callable


_MISSING = object()


class Future:
    def __init__(self, event_loop: EventLoop):
        self._event_loop = event_loop
        self._status: FutureStatus = FutureStatus._PENDING
        self._result = _MISSING
        self._exception = None
        self._callbacks: list[Callable] = []


    def set_exception(self, exc):
        match self._status:
            case FutureStatus._DONE: 
                raise ValueError("Future is already done")
            
            case FutureStatus._CANCELLED:
                raise ValueError("Future is already cancelled")
            
            case FutureStatus._PENDING:
                self._exception = exc
                self._status = FutureStatus._DONE
                for cb in list(self._callbacks):
                    self._event_loop.call_soon(cb, self)

    def set_result(self, result):
        match self._status:
            case FutureStatus._DONE: 
                raise ValueError("Future is already done")
            
            case FutureStatus._CANCELLED:
                raise ValueError("Future is already cancelled")
            
            case FutureStatus._PENDING:
                self._result = result
                self._status = FutureStatus._DONE
                
                callbacks = list(self._callbacks)
                for callback in callbacks:

                    self._event_loop.call_soon(callback, self)
    

    def result(self):
        match self._status:
            case FutureStatus._DONE: 
                if self._exception is not None:
                    raise self._exception
                
                return self._result

            case FutureStatus._CANCELLED:
                raise ValueError("Future is already cancelled")
            
            case FutureStatus._PENDING:
                raise ValueError("Future dont done")


    def done(self) -> bool:
        return self._status is FutureStatus._DONE
    

    def cancelled(self) -> bool:
        return self._status is FutureStatus._CANCELLED
    

    def add_done_callback(self, cb):
        self._callbacks.append(cb)

    def cancel(self):
        if self._status is FutureStatus._PENDING:
            self._status = FutureStatus._CANCELLED
            return True
        return False


    def __await__(self):
        if not self.done():
            yield self
        return self.result()
        



class FutureStatus(Enum):
    _PENDING = "pending"
    _DONE = "done"
    _CANCELLED = "cancelled"