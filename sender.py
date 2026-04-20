#!/usr/bin/env python3

import argparse
import asyncio
import io
import sys
from typing import Optional, Tuple

import aiofiles
import aiohttp


async def send_document(
    token: str,
    chat_id: int,
    file: bytes,
    filename: str = "document",
    max_retries: int = 50,
    retry_delay: float = 1.5,
) -> Tuple[int, str]:
    url = f"https://tapi.bale.ai/bot{token}/sendDocument"

    connector = aiohttp.TCPConnector(limit=10, limit_per_host=5, ssl=False)
    timeout = aiohttp.ClientTimeout(total=120)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        for attempt in range(max_retries + 1):
            form_data = aiohttp.FormData()
            form_data.add_field("chat_id", str(chat_id))
            form_data.add_field(
                "document",
                io.BytesIO(file),
                filename=filename,
                content_type="application/octet-stream",
            )

            try:
                async with session.post(url, data=form_data) as response:
                    response_text = await response.text()

                    if response.ok:
                        return response.status, response_text

                    print(
                        f"Attempt {attempt + 1}/{max_retries + 1} failed, "
                        f"status={response.status}, text={response_text}",
                        file=sys.stderr,
                    )
            except Exception as e:
                print(
                    f"Attempt {attempt + 1}/{max_retries + 1} exception: {e}",
                    file=sys.stderr,
                )

            if attempt < max_retries:
                await asyncio.sleep(retry_delay * (1.5 * attempt))

    return 0, "Max retries exceeded"


async def main(
    token: str,
    chat_id: int,
    name: str = "document",
    chunk_size: int = 10 * 1024 * 1024,
    max_concurrency: int = 4,
    max_retries: int = 50,
    retry_delay: float = 1.5,
    file_path: Optional[str] = None,
    use_stdin: bool = False,
    cooldown: float = 0,
) -> None:
    semaphore = asyncio.Semaphore(max_concurrency)
    queue: asyncio.Queue[Optional[tuple[bytes, str]]] = asyncio.Queue(
        maxsize=max_concurrency * 2
    )

    part = 0

    async def producer():
        nonlocal part

        if use_stdin:
            while True:
                chunk = sys.stdin.buffer.read(chunk_size)
                if not chunk:
                    await queue.put(None)
                    return
                await queue.put((chunk, f"{name}.{part}"))
                part += 1
        elif file_path:
            async with aiofiles.open(file_path, "rb") as f:
                while True:
                    chunk = await f.read(chunk_size)
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
                    status, response = await send_document(
                        token, chat_id, chunk, filename, max_retries, retry_delay
                    )
                    if status == 200:
                        print(f"Sent {len(chunk)} bytes as {filename}")
                    else:
                        print(f"Failed to send {filename}: {response}", file=sys.stderr)

                    await asyncio.sleep(cooldown)
                except Exception as e:
                    print(f"Part {filename} failed: {e}", file=sys.stderr)

            queue.task_done()

    producer_task = asyncio.create_task(producer())
    consumers = [asyncio.create_task(consumer()) for _ in range(max_concurrency)]

    await producer_task
    await asyncio.gather(*consumers, return_exceptions=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="File/STDIN to Bale :3")
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
        help="The filename base to use when uploading :3",
        required=True,
    )

    input_group = parser.add_mutually_exclusive_group(required=True)

    input_group.add_argument(
        "--stdin",
        "-i",
        action="store_true",
        help="Read input from stdin (default behavior without --file)",
    )
    input_group.add_argument(
        "--file",
        "-f",
        type=str,
        help="Path to file to read chunk by chunk",
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
    parser.add_argument(
        "--max-retries",
        "-r",
        type=int,
        help="Maximum retry attempts per chunk :3",
        default=50,
    )
    parser.add_argument(
        "--retry-delay",
        "-d",
        type=float,
        help="Initial retry delay in seconds (exponential backoff) :3",
        default=1.5,
    )
    parser.add_argument(
        "--cooldown",
        "-l",
        type=float,
        help="Cooldown delay between uploads in seconds :3",
        default=0,
    )

    args = parser.parse_args()

    asyncio.run(
        main(
            token=args.token,
            chat_id=args.chat_id,
            name=args.name,
            chunk_size=args.chunk_size,
            max_concurrency=args.concurrency,
            max_retries=args.max_retries,
            retry_delay=args.retry_delay,
            file_path=args.file,
            use_stdin=args.stdin,
            cooldown=args.cooldown,
        )
    )
