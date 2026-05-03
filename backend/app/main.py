from fastapi import FastAPI
from app.api.routes import router
from fastapi.responses import FileResponse
from pathlib import Path
app = FastAPI(title="FlowAutomate AI")

app.include_router(router)

BASE_DIR = Path(__file__).resolve().parent.parent

@app.get("/download/{filename}")
def download_file(filename: str):
    file_path = BASE_DIR / "data" / filename

    if not file_path.exists():
        return {"error": f"File not found: {filename}"}

    return FileResponse(
        path=file_path,
        filename=filename,
        media_type="application/octet-stream"
    )