import os
import asyncio
import micropip


async def main():
    for filename in os.listdir("dist"):
        if filename.endswith(".whl"):
            await micropip.install([f"./dist/{filename}"])

asyncio.run(main())
