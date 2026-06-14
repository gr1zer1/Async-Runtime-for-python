from async_runtime import EventLoop

async def inner():
    return 10

async def main():
    result = await inner()
    return result * 2

loop = EventLoop()
print(loop.run_until_complete(main()))