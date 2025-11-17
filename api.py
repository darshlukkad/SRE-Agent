from fastapi import FastAPI

app = FastAPI()


@app.get("/version")
def get_version():
    """Mock API endpoint returning application status and site version."""
    return {
        "Application Status": "OK",
        "Site version": "v1.2.3"
    }
