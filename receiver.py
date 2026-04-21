import asyncio
import os
import sys
from typing import Dict, Optional, Set

import aiofiles
import aiohttp
from aiobale import Client, Dispatcher  # type: ignore
from aiobale.types import Message  # type: ignore

CHAT_IDS: Set[int] = set()

dp = Dispatcher()
bale_client = Client(dp)

connector = aiohttp.TCPConnector(limit=10, limit_per_host=5, ssl=False)


async def download_file(
    file_id: int,
    access_hash: int,
    filename: str,
    max_retries: int = 50,
    retry_delay: float = 1.5,
) -> Optional[str]:
    temp_filename = f"{filename}.tmp"
    timeout = aiohttp.ClientTimeout(total=120)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        for attempt in range(max_retries + 1):
            try:
                file_info = await bale_client.get_file(file_id, access_hash)

                if file_info is None or not file_info.url:
                    raise ValueError("File not found or URL missing")

                url = file_info.url

                headers: Dict[str, str] = dict()

                if attempt > 0 and os.path.exists(temp_filename):
                    current_size = os.path.getsize(temp_filename)

                    if current_size > 0:
                        headers["Range"] = f"bytes={current_size}-"
                        print(f"Resuming from byte {current_size}")
                        file_mode = "ab"
                    else:
                        file_mode = "wb"
                else:
                    file_mode = "wb"

                print(f"Download attempt {attempt + 1}/{max_retries + 1}: {url}")

                async with session.get(url, headers=headers) as response:
                    if headers.get("Range"):
                        if response.status == 206:
                            print("Server accepted resume")
                        elif response.status == 200:
                            print("Server ignored Range header, restarting")
                            file_mode = "wb"
                        elif response.status == 416:
                            print("File already fully downloaded")
                            os.replace(temp_filename, filename)
                            return filename

                        if response.status in (500, 403) and os.path.exists(
                            temp_filename
                        ):
                            os.remove(temp_filename)
                            print(
                                f"Deleted partial file {temp_filename} due to HTTP status."
                                "will restart from beginning",
                                file=sys.stderr,
                            )

                        else:
                            raise aiohttp.ClientResponseError(
                                response.request_info,
                                response.history,
                                status=response.status,
                                message=f"Unexpected status {response.status}",
                            )
                    elif response.status != 200:
                        raise aiohttp.ClientResponseError(
                            response.request_info,
                            response.history,
                            status=response.status,
                            message=f"HTTP {response.status}",
                        )

                    async with aiofiles.open(temp_filename, file_mode) as f:
                        if file_mode == "wb":
                            await f.truncate(0)
                        async for chunk in response.content.iter_chunked(1024 * 64):
                            await f.write(chunk)

                os.replace(temp_filename, filename)
                print(f"Download finished: {filename}")
                return filename

            except Exception as e:
                print(
                    f"Attempt {attempt + 1}/{max_retries + 1} failed: {e}",
                    file=sys.stderr,
                )

                if attempt >= max_retries:
                    print("Max retries exceeded")
                    return

                delay = retry_delay * (1.5 * attempt)
                print(f"Retrying in {delay:.2f} seconds...", file=sys.stderr)
                await asyncio.sleep(delay)


@dp.message(lambda m: m.chat.id in CHAT_IDS)  # type: ignore
async def msg_handler(m: Message) -> None:
    if m.text == "/download":
        if m.replied_to is None or m.replied_to.document is None:
            await m.reply("Please reply to a message with a document present.")
            return

        doc = m.replied_to.document

        await download_file(
            doc.file_id,
            doc.access_hash,
            str(doc.name or "document.bin"),  # type: ignore
        )
        await m.reply(f"Downloaded {doc.name}")  # type: ignore
        return

    if m.document:
        await download_file(
            m.document.file_id,
            m.document.access_hash,
            str(m.document.name or "document.bin"),  # type: ignore
        )


async def main() -> None:
    await bale_client.start()  # type: ignore


if __name__ == "__main__":
    asyncio.run(main())
