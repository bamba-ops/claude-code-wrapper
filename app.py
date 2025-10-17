import asyncio
import json
from contextlib import suppress
from typing import AsyncGenerator, Optional

from fastapi import FastAPI, HTTPException, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field


HEARTBEAT_INTERVAL_SECONDS = 15
CLAUDE_BINARY = "claude"


class RunRequest(BaseModel):
    prompt: str = Field(..., min_length=1)


app = FastAPI()

# Allow all origins by default. Adjust if a stricter policy is required.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


async def _terminate_process(process: asyncio.subprocess.Process) -> None:
    if process.returncode is not None:
        return

    process.terminate()
    try:
        await asyncio.wait_for(process.wait(), timeout=5)
    except asyncio.TimeoutError:
        process.kill()
        await process.wait()


@app.post("/runs")
async def create_run(request: Request, run_request: RunRequest) -> StreamingResponse:
    prompt = run_request.prompt
    if not prompt or not prompt.strip():
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Prompt must not be empty.",
        )

    try:
        process = await asyncio.create_subprocess_exec(
            CLAUDE_BINARY,
            "-p",
            prompt,
            "--output-format=stream-json",
            "--include-partial-messages",
            "--permission-mode",
            "plan",
            "--verbose",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
    except FileNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Claude CLI executable not found on the server.",
        ) from exc

    queue: asyncio.Queue[Optional[str]] = asyncio.Queue()
    sentinel = None

    async def stream_stdout() -> None:
        assert process.stdout is not None
        while True:
            line = await process.stdout.readline()
            if not line:
                break
            payload = line.decode("utf-8", errors="replace").rstrip("\r\n")
            await queue.put(f"data: {payload}\n\n")

    async def stream_stderr() -> None:
        assert process.stderr is not None
        while True:
            line = await process.stderr.readline()
            if not line:
                break
            message = line.decode("utf-8", errors="replace").rstrip("\r\n")
            stderr_payload = json.dumps({"type": "stderr", "message": message})
            await queue.put(f"data: {stderr_payload}\n\n")

    async def send_heartbeats() -> None:
        try:
            while True:
                await asyncio.sleep(HEARTBEAT_INTERVAL_SECONDS)
                await queue.put(":\n\n")
        except asyncio.CancelledError:
            # Heartbeat task cancelled once processing concludes.
            raise

    async def monitor_process(
        stdout_task: asyncio.Task[None],
        stderr_task: asyncio.Task[None],
        heartbeat_task: asyncio.Task[None],
    ) -> None:
        returncode = await process.wait()
        heartbeat_task.cancel()
        with suppress(asyncio.CancelledError):
            await heartbeat_task

        with suppress(asyncio.CancelledError):
            await stdout_task

        with suppress(asyncio.CancelledError):
            await stderr_task

        if returncode != 0:
            completion_payload = json.dumps(
                {"type": "completed", "code": returncode}
            )
            await queue.put(f"data: {completion_payload}\n\n")

        await queue.put(sentinel)

    stdout_task = asyncio.create_task(stream_stdout())
    stderr_task = asyncio.create_task(stream_stderr())
    heartbeat_task = asyncio.create_task(send_heartbeats())
    monitor_task = asyncio.create_task(
        monitor_process(stdout_task, stderr_task, heartbeat_task)
    )

    async def event_stream() -> AsyncGenerator[str, None]:
        try:
            while True:
                if await request.is_disconnected():
                    await _terminate_process(process)
                    break

                try:
                    chunk = await asyncio.wait_for(queue.get(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue

                if chunk is sentinel:
                    break

                assert chunk is not None
                yield chunk
        finally:
            for task in (stdout_task, stderr_task, heartbeat_task, monitor_task):
                task.cancel()
            await asyncio.gather(
                stdout_task,
                stderr_task,
                heartbeat_task,
                monitor_task,
                return_exceptions=True,
            )
            await _terminate_process(process)

    headers = {
        "Content-Type": "text/event-stream",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
    }

    return StreamingResponse(event_stream(), headers=headers)
