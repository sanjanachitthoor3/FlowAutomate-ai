import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)


def clean_data(file: str) -> dict:
    """
    Read a CSV file, apply basic cleaning operations, and save the result.

    Cleaning steps (applied in order):
      1. Drop any row that contains at least one null / NaN value.
      2. Remove fully duplicate rows (all columns identical).
      3. Strip leading/trailing whitespace from every string column.

    Args:
        file: Path to the input CSV file (e.g. "data/sample.csv").

    Returns:
        {"file": "<path_to_cleaned_csv>"}
        e.g. {"file": "data/sample_cleaned.csv"}

    Raises:
        FileNotFoundError: if the input file does not exist.
        ValueError:        if the file cannot be parsed as CSV.
    """
    input_path = Path(file)

    # --- Guard: file must exist before any work begins ---
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {file!r}")

    # --- Read CSV ---
    try:
        df = pd.read_csv(input_path)
    except Exception as exc:
        raise ValueError(f"Could not parse {file!r} as a CSV file: {exc}") from exc

    logger.debug("Loaded %d rows × %d columns from %r", len(df), len(df.columns), file)


##----------------CLEANING STEPS----------------##
    # --- Step 1: strip whitespace FIRST ---
    str_cols = df.select_dtypes(include="object").columns
    df[str_cols] = df[str_cols].apply(lambda col: col.str.strip())

    # --- Step 2: drop rows that are mostly empty (generic approach) ---
    df = df.dropna(thresh=max(1, int(0.7 * len(df.columns))))

    # --- Step 3: remove duplicates AFTER cleaning ---
    df = df.drop_duplicates()
##-----------------------------------------------


    logger.debug("After cleaning: %d rows × %d columns", len(df), len(df.columns))

    # --- Build output path: "data/sample.csv" → "data/sample_cleaned.csv" ---
    output_path = input_path.with_name(f"{input_path.stem}_cleaned{input_path.suffix}")

    # --- Save cleaned data ---
    df.to_csv(output_path, index=False)

    logger.info("Cleaned file saved to %r", str(output_path))

    return {"file": output_path.as_posix()}