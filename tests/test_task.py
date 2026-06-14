from async_runtime import Task, EventLoop
import pytest

async def get_data(): return 10

async def get_none(): return None


async def main(func): return await func()


@pytest.fixture
def loop(): 
    loop = EventLoop()
    yield loop


def test_coro_get_result(loop):
    result = loop.run_until_complete(main(get_data))

    assert result == 10


def test_coro_get_none(loop):
    result = loop.run_until_complete(main(get_none))

    assert result is None