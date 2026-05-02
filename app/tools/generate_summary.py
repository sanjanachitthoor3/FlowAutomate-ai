import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)


def generate_summary(file: str) -> dict:
    """
    Read a CSV file, compute a statistical summary, and save it to disk.

    The summary is produced by pandas' describe(include='all'), which covers
    both numeric columns (count, mean, std, min, max, percentiles) and
    categorical columns (count, unique, top, freq).

    Args:
        file: Path to the input CSV file (e.g. "data/sample.csv").

    Returns:
        {"file": "<path_to_summary_csv>"}
        e.g. {"file": "data/sample_summary.csv"}

    Raises:
        FileNotFoundError: if the input CSV does not exist on disk.
        ValueError:        if the file exists but cannot be parsed as CSV.
    """
    input_path = Path(file)

    # --- Guard: input file must exist before we do any work ---
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {file!r}")

    # --- Read the CSV ---
    try:
        df = pd.read_csv(input_path)
    except Exception as exc:
        raise ValueError(f"Could not parse {file!r} as a CSV file: {exc}") from exc

    logger.debug("Loaded %d rows × %d columns from %r", len(df), len(df.columns), file)

    # --- Generate summary ---
    # include='all' ensures both numeric and object/categorical columns are
    # described; purely numeric calls would silently drop text columns.
    summary_df = df.describe(include="all").fillna("")

    # --- Build output path: "data/sample.csv" → "data/sample_summary.csv" ---
    output_path = input_path.with_name(f"{input_path.stem}_summary{input_path.suffix}")

    # --- Save summary CSV ---
    # index=True keeps the describe() row labels (count, mean, std, …) as a column.
    summary_df.to_csv(output_path, index=True)

    logger.info("Summary saved to %r", str(output_path))

    return {"file": output_path.as_posix()}