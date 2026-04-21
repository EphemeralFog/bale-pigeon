# File to Bale

A tool to transfer files to Bale without pain, using either file paths or directly from stdin.

## Overview

File to Bale bridges external files to your internal network using Bale Messenger. It consists of two tightly coupled components:

- **`uploader.py`** – Runs on an external server (outside the country). Watches for files or accepts stdin input and sends them to Bale.
- **`receiver.py`** – Runs on an internal server (inside the country). Listens for messages via Bale and saves the received files locally.

## Installation

### Using `uv` (Recommended)

[`uv`](https://github.com/astral-sh/uv) is a fast Python package manager. It handles the separate dependencies for uploader and receiver cleanly.

**Uploader (External Server):**
```bash
uv venv --seed .venv-uploader
source .venv-uploader/bin/activate
uv pip install -e ".[uploader]"
```

**Receiver (Internal Server):**
```bash
uv venv --seed .venv-receiver
source .venv-receiver/bin/activate
uv pip install -e ".[receiver]"
```

### Without `uv` (Standard pip + venv)

If you prefer standard Python tooling, you can use `pip` and `venv` directly.

**Uploader (External Server):**
```bash
python -m venv .venv-uploader
source .venv-uploader/bin/activate
pip install -e ".[uploader]"
```

**Receiver (Internal Server):**
```bash
python -m venv .venv-receiver
source .venv-receiver/bin/activate
pip install -e ".[receiver]"
```

### Manual Dependency Installation

If you prefer not to use editable installs (`-e`), install dependencies directly:

**Uploader:**
```bash
pip install aiohttp aiofiles
```

**Receiver:**
```bash
pip install aiohttp aiobale
```
