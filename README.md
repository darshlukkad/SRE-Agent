# SRE-Agent (Streamlit demo)

This is a small demo Streamlit application that demonstrates:
- Authentication (hardcoded): `Admin` / `secret`
- A mock API (`/version`) served by `api.py` (used in-process via TestClient)
- Redis-backed caching of dummy data (with a fallback in-memory store if Redis is not available)
- Five UI actions (buttons) described below

## Actions
1. Deployment version: calls the mock API and displays `Application Status` and `Site version`.
2. Site Active Users: displays `Application status`, `Number of Concurrent Users`, `Number of Active Sessions`, `Users List`, and `Time since online`.
3. Active connected devices: displays `Application status`, `Number of Concurrent Devices`, `Number of Active Sensors`, `Sensor Device List` and `Time since online`.
4. Site image intelligence: a text box where you enter a natural language query — the app runs a simple RAG-style retrieval over stored docs and (if `OPENAI_API_KEY` is set) will try to call OpenAI; otherwise it synthesizes an answer.
5. Simulate Failure: updates cached application status to `changed to region 2` and sets `application region`.

## Setup

1. Create a Python environment (recommended):

## Docker

A Docker setup is included to run the Streamlit app alongside Redis.

Build and run with Compose:

```bash
docker compose up --build
```

Or on older Docker Compose installations:

```bash
docker-compose up --build
```

The app will be available at `http://localhost:8501` (login: `Admin` / `secret`).

To stop and remove containers:

```bash
docker compose down
```

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

2. (Optional) Run Redis for persistence. If you don't have Redis, the app will use an in-memory fallback.

On macOS with Homebrew:

```bash
brew install redis
brew services start redis
```

3. Run the app:

```bash
streamlit run app.py
```

Login with username `Admin` and password `secret`.

## Notes
- The mock API is implemented in `api.py` and is called in-process (no separate server required).
- If you want richer RAG answers, set `OPENAI_API_KEY` in your environment.
# SRE-Agent
