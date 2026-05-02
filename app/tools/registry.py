from typing import Callable
from app.tools.clean_data import clean_data
from app.tools.rename_files import rename_files
from app.tools.generate_summary import generate_summary

TOOL_REGISTRY: dict[str, Callable] = {
    "clean_data":       clean_data,
    "rename_files":     rename_files,
    "generate_summary": generate_summary,
}