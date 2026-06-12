from async_runtime import EventLoop

async def main():
    return 42

loop = EventLoop()
result = loop.run_until_complete(main())
print(result)  # 42