# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Claude Code wrapper project that provides a FastAPI-based web service interface to the Claude CLI. The project consists of two main components:

- **app.py**: FastAPI server that exposes a `/runs` endpoint for streaming Claude CLI responses
- **client.py**: Command-line client for interacting with the FastAPI server

## Architecture

### Server (app.py)
- FastAPI application with CORS middleware enabled
- Single endpoint `/runs` that accepts POST requests with prompts
- Executes the Claude CLI (`claude` binary) as a subprocess
- Streams both stdout and stderr as Server-Sent Events (SSE)
- Includes heartbeat mechanism to keep connections alive
- Handles process lifecycle management and cleanup

### Client (client.py)
- Command-line interface for sending prompts to the server
- Parses and displays streaming SSE responses
- Handles JSON parsing and error reporting

## Development Setup

### Dependencies
- Python 3.12.3
- Virtual environment located at `.venv/`
- Key packages: FastAPI, Pydantic, Requests, Uvicorn

### Running the Server
```bash
# Activate virtual environment
source .venv/bin/activate

# Start the FastAPI server
uvicorn app:app --host 0.0.0.0 --port 8000 --reload
```

### Running the Client
```bash
# Activate virtual environment
source .venv/bin/activate

# Send a prompt to the server
python client.py "Your prompt here"

# Or specify a custom URL
python client.py "Your prompt here" --url http://localhost:8000/runs
```

## Key Configuration

- **CLAUDE_BINARY**: Set to "claude" - assumes the Claude CLI is installed and available in PATH
- **HEARTBEAT_INTERVAL_SECONDS**: 15 seconds for SSE heartbeat
- **Default URL**: http://127.0.0.1:8000/runs

## API Endpoints

### POST /runs
- **Request Body**: `{"prompt": "string"}`
- **Response**: Server-Sent Events stream with JSON payloads
- **Stream Format**: Includes stdout data, stderr messages, and completion status

## Error Handling

- Server returns 422 for empty prompts
- Server returns 500 if Claude CLI executable is not found
- Client handles connection errors and server error responses
- Process cleanup ensures subprocess termination on client disconnect