import streamlit as st
import time
import json
import os
from typing import List

# Attempt to use real redis; fall back to simple in-memory store if Redis not available.
try:
    import redis
    _redis = redis.Redis(host=os.getenv("REDIS_HOST", "localhost"), port=int(os.getenv("REDIS_PORT", 6379)), db=0, decode_responses=True)
    _redis.ping()
    class RedisClient:
        def __init__(self, r):
            self._r = r

        def get(self, k):
            return self._r.get(k)

        def set(self, k, v):
            return self._r.set(k, v)

        def lpush(self, k, *vals):
            return self._r.lpush(k, *vals)

        def lrange(self, k, a, b):
            return self._r.lrange(k, a, b)

        def exists(self, k):
            return self._r.exists(k)

        def delete(self, k):
            return self._r.delete(k)

    cache = RedisClient(_redis)
    redis_available = True
except Exception:
    # Minimal in-memory fallback using Streamlit session state
    redis_available = False

    # Initialize session state for cache if not exists
    if 'fake_redis_cache' not in st.session_state:
        st.session_state.fake_redis_cache = {}

    class FakeRedis:
        def __init__(self):
            self._d = st.session_state.fake_redis_cache

        def get(self, k):
            return self._d.get(k)

        def set(self, k, v):
            self._d[k] = v
            st.session_state.fake_redis_cache = self._d

        def lpush(self, k, *vals):
            self._d.setdefault(k, [])[0:0] = list(vals)
            st.session_state.fake_redis_cache = self._d

        def lrange(self, k, a, b):
            arr = self._d.get(k, [])
            return arr[a:b+1 if b!=-1 else None]

        def exists(self, k):
            return 1 if k in self._d else 0

        def delete(self, k):
            if k in self._d:
                del self._d[k]
                st.session_state.fake_redis_cache = self._d

    cache = FakeRedis()

from api import app as api_app
from starlette.testclient import TestClient

client = TestClient(api_app)


def seed_dummy_data():
    # Initialize region if not exists
    if not cache.exists("site:app_region"):
        cache.set("site:app_region", json.dumps("region-1"))

    # Users
    if not cache.exists("site:users"):
        users = ["alice", "bob", "carol", "dave"]
        cache.set("site:app_status", json.dumps("OK"))
        cache.set("site:concurrent_users", json.dumps(len(users)))
        cache.set("site:active_sessions", json.dumps(3))
        cache.set("site:users", json.dumps(users))
        cache.set("site:times", json.dumps([120, 300, 45]))

    # Devices
    if not cache.exists("site:devices"):
        devices = ["Turbine-1", "Thermal-2", "Rotor-3", "OilGas-4"]
        cache.set("site:device_status", json.dumps("OK"))
        cache.set("site:concurrent_devices", json.dumps(len(devices)))
        cache.set("site:active_sensors", json.dumps(4))
        cache.set("site:devices", json.dumps(devices))
        cache.set("site:device_times", json.dumps([300, 600, 300, 600]))

    # Documents for RAG
    if not cache.exists("site:docs"):
        docs = [
            "The site operates turbomachinery and thermal systems.",
            "Maintenance cycles are every 6 months for rotors.",
            "Operational safety requires sensors to be calibrated monthly.",
        ]
        cache.set("site:docs", json.dumps(docs))


seed_dummy_data()


def check_auth(username: str, password: str) -> bool:
    return username == "Admin" and password == "secret"


def run_deployment_version():
    # Call the mock API using TestClient (runs in-process)
    resp = client.get("/version")
    if resp.status_code == 200:
        data = resp.json()
        # Add region information
        region = json.loads(cache.get("site:app_region") or '"region-1"')
        data["Region"] = region
        return data
    return {"Application Status": "ERROR", "Site version": "unknown"}


def get_site_active_users():
    status = json.loads(cache.get("site:app_status") or '"UNKNOWN"')
    num = json.loads(cache.get("site:concurrent_users") or '0')
    sessions = json.loads(cache.get("site:active_sessions") or '0')
    users = json.loads(cache.get("site:users") or '[]')
    times = json.loads(cache.get("site:times") or '[]')
    region = json.loads(cache.get("site:app_region") or '"region-1"')

    # Get Kafka-sourced metrics
    server_cpu = json.loads(cache.get("site:server_cpu") or '0')
    server_memory = json.loads(cache.get("site:server_memory") or '0')
    avg_latency = json.loads(cache.get("site:avg_latency") or '0')
    last_update = cache.get("site:last_update") or "N/A"

    return {
        #"Application status": status,
        "Region": region,
        "Number of Concurrent Users": num,
        "Number of Active Sessions": sessions,
        "Server CPU (%)": server_cpu,
        "Server Memory (GB)": server_memory,
        "Average Latency (ms)": avg_latency,
        "Last Update (UTC)": last_update,
        "Users List": users,
        "Time since online": times,
    }


def get_active_connected_devices():
    status = json.loads(cache.get("site:device_status") or '"UNKNOWN"')
    num = json.loads(cache.get("site:concurrent_devices") or '0')
    sensors = json.loads(cache.get("site:active_sensors") or '0')
    devs = json.loads(cache.get("site:devices") or '[]')
    times = json.loads(cache.get("site:device_times") or '[]')
    region = json.loads(cache.get("site:app_region") or '"region-1"')
    last_update = cache.get("site:device_last_update") or "N/A"

    return {
        "Application status": status,
        "Region": region,
        "Number of Concurrent Devices": num,
        "Number of Active Sensors": sensors,
        "Last Update (UTC)": last_update,
        "Sensor Device List": devs,
        "Time since online (seconds)": times,
    }


def rag_query(query: str) -> str:
    # Very simple retrieval from stored docs; optionally call external LLM if OPENAI_API_KEY exists
    docs = json.loads(cache.get("site:docs") or '[]')
    found = [d for d in docs if any(tok.lower() in d.lower() for tok in query.split())]
    if not found:
        found = docs

    # Try to use OpenAI if key present, otherwise synthesize answer
    openai_key = os.getenv("OPENAI_API_KEY")
    if openai_key:
        try:
            import openai
            openai.api_key = openai_key
            prompt = f"Answer the query using these docs:\n\nDocs:\n{json.dumps(found, indent=2)}\n\nQuery: {query}\n\nAnswer:"
            resp = openai.ChatCompletion.create(model="gpt-4o-mini", messages=[{"role":"user","content":prompt}], max_tokens=300)
            return resp.choices[0].message.content
        except Exception:
            pass

    # Synthesize a simple answer
    return "\n--- Retrieved documents ---\n" + "\n".join(found) + f"\n\n(Answer synthesized for query: {query})"


def simulate_failure():
    # Toggle between region 1 and region 2
    current_region = json.loads(cache.get("site:app_region") or '"region-1"')

    if current_region == "region-1":
        new_region = "region-2"
        cache.set("site:app_status", json.dumps("Failover to region-2"))
        cache.set("site:app_region", json.dumps(new_region))
        return {"Application status": "Failover to region-2", "application region": new_region}
    else:
        new_region = "region-1"
        cache.set("site:app_status", json.dumps("Failback to region-1"))
        cache.set("site:app_region", json.dumps(new_region))
        return {"Application status": "Failback to region-1", "application region": new_region}


def main():
    st.title("SRE-Agent Demo (Streamlit)")

    if "authenticated" not in st.session_state:
        st.session_state.authenticated = False

    if not st.session_state.authenticated:
        st.subheader("Login")
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")
        if st.button("Login"):
            if check_auth(username, password):
                st.session_state.authenticated = True
                st.rerun()
            else:
                st.error("Invalid credentials")
        st.stop()

    st.markdown("### Actions")

    # Create button row
    col1, col2, col3, col4, col5 = st.columns(5)

    with col1:
        btn_deployment = st.button("Deployment Version", use_container_width=True)
    with col2:
        btn_users = st.button("Site Active Users", use_container_width=True)
    with col3:
        btn_devices = st.button("Connected Devices", use_container_width=True)
    with col4:
        btn_rag = st.button("Site Intelligence", use_container_width=True)
    with col5:
        btn_failure = st.button("Simulate Failure", use_container_width=True)

    st.markdown("---")

    # Results area
    if btn_deployment:
        with st.spinner("Calling deployment API..."):
            data = run_deployment_version()
            st.success("Deployment version fetched")
            st.json(data)

    if btn_users:
        with st.spinner("Loading site active users..."):
            data = get_site_active_users()
            st.json(data)

    if btn_devices:
        with st.spinner("Loading devices..."):
            data = get_active_connected_devices()
            st.json(data)

    if btn_rag:
        query = st.text_area("Enter natural language query for site image intelligence")
        if st.button("Run Query"):
            if not query.strip():
                st.error("Please type a query first")
            else:
                with st.spinner("Running retrieval and generation..."):
                    answer = rag_query(query)
                    st.markdown("**RAG Result**")
                    st.text(answer)

    if btn_failure:
        with st.spinner("Simulating failure and updating cache..."):
            out = simulate_failure()
            st.success(f"Region switched to {out['application region']}")
            st.json(out)
            # Wait a moment then refresh to show updated state
            time.sleep(0.5)
            st.rerun()

    st.markdown("---")
    st.markdown("### Current System State")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("App Status", json.loads(cache.get("site:app_status") or '"UNKNOWN"'))
        st.metric("Concurrent Users", json.loads(cache.get("site:concurrent_users") or '0'))
    with col2:
        st.metric("Region", json.loads(cache.get("site:app_region") or '"region-1"'))
        st.metric("Concurrent Devices", json.loads(cache.get("site:concurrent_devices") or '0'))
    with col3:
        st.metric("Redis Available", str(redis_available))


if __name__ == "__main__":
    main()
