import logging
from pathlib import Path

logger = logging.getLogger(__name__)


def rename_files(file: str, new_name: str) -> dict:
    """
    Rename a file on disk, keeping it in its original directory.

    Only the filename is replaced — the parent directory is always
    preserved. So renaming "data/sample_summary.csv" to "summary.csv"
    produces "data/summary.csv", not "./summary.csv".

    Args:
        file:     Path to the existing file (e.g. "data/sample_summary.csv").
        new_name: Bare filename (with extension) for the renamed file
                  (e.g. "summary.csv"). Directory components are ignored.

    Returns:
        {"file": "<new_file_path>"}
        e.g. {"file": "data/summary.csv"}

    Raises:
        FileNotFoundError: if `file` does not exist on disk.
        FileExistsError:   if a file named `new_name` already exists in
                           the same directory (safe — no silent overwrites).
        ValueError:        if `new_name` is empty or blank.
    """
    source = Path(file)

    # --- Guard: source must exist ---
    if not source.exists():
        raise FileNotFoundError(f"Source file not found: {file!r}")

    # --- Guard: new_name must be a non-empty filename ---
    bare_name = new_name.strip()
    if not bare_name:
        raise ValueError("new_name must not be empty.")

    # Build the destination path: same parent directory, new filename only.
    # Path(bare_name).name strips any accidental directory prefix the caller
    # might have included (e.g. "subdir/summary.csv" → "summary.csv").
    destination = source.parent / Path(bare_name).name

    # --- Guard: do not silently clobber an existing file ---
    if destination.exists():
        raise FileExistsError(
            f"A file named {str(destination)!r} already exists. "
            "Delete or move it before renaming."
        )

    # --- Rename ---
    # Path.rename() is atomic on POSIX systems when src and dst are on the
    # same filesystem, which is the common case here (same parent directory).
    source.rename(destination)

    logger.info("Renamed %r → %r", str(source), str(destination))

    return {"file": destination.as_posix()}