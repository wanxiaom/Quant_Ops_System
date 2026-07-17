import asyncio
import websockets

async def listen():
    uri = "ws://--/"
    # websockets 15+ 默认读取 HTTP_PROXY；本机 Manager 必须直连。
    async with websockets.connect(uri, proxy=None) as ws:
        print("Connected to WS")
        async for msg in ws:
            print(msg)

if __name__ == "__main__":
    asyncio.run(listen())
