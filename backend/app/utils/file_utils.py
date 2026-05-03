from pathlib import Path

def resolve_file_path(file: str) -> Path:
    path = Path(file)

    if path.exists():
        return path

    data_path = Path("data") / file
    if data_path.exists():
        return data_path

    raise FileNotFoundError(f"Input file not found: {file!r}")