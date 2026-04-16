#!/usr/bin/env python3

import argparse
import asyncio
import io
import sys
from typing import Tuple

import aiohttp


async def send_document(
    token: str, chat_id: int, file: bytes, filename: str = "document"
) -> Tuple[int, str]:
    url = f"https://tapi.bale.ai/bot{token}/sendDocument"

    form_data = aiohttp.FormData()
    form_data.add_field("chat_id", str(chat_id))
    form_data.add_field(
        "document",
        io.BytesIO(file),
        filename=filename,
        content_type="application/octet-stream",
    )

    async with aiohttp.ClientSession() as session:
        async with session.post(url, data=form_data) as response:
            response_text = await response.text()
            if not response.ok:
                print(
                    f"API error, status={response.status} text={response_text}",
                    file=sys.stderr,
                )
            return response.status, response_text


async def process_chunk(
    token: str,
    chunk: bytes,
    name: str,
    chat_id: int,
) -> None:
    await send_document(token, chat_id, chunk, name)
    print(f"Sent {len(chunk)} bytes, filename={name}")


async def main(
    token: str,
    chat_id: int,
    name: str = "document",
    chunk_size: int = 15 * 1024 * 1024,
    max_concurrency: int = 4,
) -> None:
    semaphore = asyncio.Semaphore(max_concurrency)
    queue: asyncio.Queue = asyncio.Queue()

    part = 0

    async def producer():
        nonlocal part
        while True:
            chunk = sys.stdin.buffer.read(chunk_size)

            if not chunk:
                await queue.put(None)
                return

            await queue.put((chunk, f"{name}.{part}"))
            part += 1

    async def consumer():
        while True:
            item = await queue.get()

            if item is None:
                return

            chunk, filename = item

            async with semaphore:
                try:
                    await process_chunk(token, chunk, filename, chat_id)
                except Exception as e:
                    print(f"Part {filename} failed: {e}", file=sys.stderr)

            queue.task_done()

    producer_task = asyncio.create_task(producer())
    consumers = [asyncio.create_task(consumer()) for _ in range(max_concurrency)]

    await producer_task
    await asyncio.gather(*consumers, return_exceptions=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Standard input to Bale :3")
    parser.add_argument(
        "--chat-id",
        "-c",
        type=int,
        help="The chat id to send the files in :3",
        required=True,
    )
    parser.add_argument(
        "--token",
        "-t",
        type=str,
        help="Bot token for sending the files into the chat id :3",
        required=True,
    )
    parser.add_argument(
        "--name",
        "-n",
        type=str,
        help="The filename to use when uploading :3",
        required=True,
    )
    parser.add_argument(
        "--chunk-size",
        "-s",
        type=int,
        help="Chunk size for large files in bytes :3",
        default=(15 * 1024 * 1024),
    )
    parser.add_argument(
        "--concurrency",
        "-o",  # I'm aware that this argument does not mean anything and does not make sense.
        type=int,
        help="Concurrent file chunk processing :3",
        default=1,
    )

    args = parser.parse_args()

    asyncio.run(
        main(
            token=args.token,
            chat_id=args.chat_id,
            name=args.name,
            chunk_size=args.chunk_size,
            max_concurrency=args.concurrency,
        )
    )
