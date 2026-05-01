import logging
from typing import Any, Callable

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Tool implementations  (stub logic — replace with real business logic)
# ---------------------------------------------------------------------------

def clean_data(file: str, **kwargs) -> dict:
    """Remove nulls, strip whitespace, deduplicate rows in a CSV file."""
    # TODO: replace with actual pandas / polars cleaning logic
    logger.debug("clean_data called on %r", file)
    return {"cleaned_file": file, "rows_removed": 0, "status": "cleaned"}


def rename_files(file: str, new_name: str = "", **kwargs) -> dict:
    """Rename a file on disk."""
    # TODO: replace with os.rename / pathlib logic
    logger.debug("rename_files called: %r -> %r", file, new_name)
    target = new_name or file.replace(".", "_renamed.")
    return {"original": file, "renamed_to": target, "status": "renamed"}


def generate_summary(file: str, **kwargs) -> dict:
    """Produce a statistical / textual summary of a data file."""
    # TODO: replace with real summarisation logic
    logger.debug("generate_summary called on %r", file)
    return {"file": file, "rows": 0, "columns": 0, "summary": "stub summary"}


# ---------------------------------------------------------------------------
# Tool registry
# ---------------------------------------------------------------------------

# Single source of truth: maps every allowed tool name to its callable.
# Adding a new tool = one line here + the function above.
TOOL_REGISTRY: dict[str, Callable] = {
    "clean_data":       clean_data,
    "rename_files":     rename_files,
    "generate_summary": generate_summary,
}


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _make_log_entry(
    step: int,
    tool: str,
    status: str,
    result: Any = None,
    error: str = "",
) -> dict:
    """Build a structured log entry for a single execution step."""
    return {
        "step":   step,
        "tool":   tool,
        "status": status,       # "success" | "failed" | "skipped"
        "result": result,
        "error":  error,
    }


def _merge_state(args: dict, state: dict) -> dict:
    """
    Merge the current execution state into a step's args.

    State values only fill in *missing* keys — explicit args always win.
    This lets the output of step N (e.g. a cleaned file path) flow
    automatically into step N+1 without the plan author having to wire
    them manually.
    """
    merged = {**state, **args}   # args override state for clashing keys
    return merged


# ---------------------------------------------------------------------------
# Public interface
# ---------------------------------------------------------------------------

def execute_plan(plan: list) -> dict:
    """
    Execute a validated workflow plan step-by-step.

    For each step the engine:
      1. Looks up the tool function in TOOL_REGISTRY.
      2. Merges accumulated execution state into the step's args so
         outputs from earlier steps are available to later ones.
      3. Calls the tool and stores its result.
      4. Appends a structured log entry.
      5. On any failure, stops immediately and returns an error response.

    Args:
        plan: A validated list of step dicts from the Plan Parser.
              Each dict must have: "step" (int), "tool" (str), "args" (dict).

    Returns:
        {
            "status":  "success" | "failed",
            "logs":    [ { step, tool, status, result, error }, ... ],
            "results": [ <tool return value>, ... ]   # only successful steps
        }
    """
    logs:    list[dict] = []
    results: list[Any]  = []

    # Shared state bucket: populated with each tool's output so subsequent
    # steps can reference values produced by earlier ones (e.g. "file").
    execution_state: dict = {}

    for step_def in plan:
        step_num = step_def.get("step", "?")
        tool_name = step_def.get("tool", "")
        raw_args  = dict(step_def.get("args") or {})

        logger.info("Executing step %s — tool: %r", step_num, tool_name)

        # --- Gate: tool must exist in registry ---
        if tool_name not in TOOL_REGISTRY:
            msg = f"Unknown tool '{tool_name}' — not found in TOOL_REGISTRY."
            logger.error("Step %s failed: %s", step_num, msg)
            logs.append(_make_log_entry(step_num, tool_name, "failed", error=msg))
            # Hard stop: return immediately so a bad plan never partially executes.
            return {"status": "failed", "logs": logs, "results": results}

        # --- Merge state into args (state fills gaps, args always win) ---
        merged_args = _merge_state(raw_args, execution_state)

        # --- Execute ---
        try:
            tool_fn = TOOL_REGISTRY[tool_name]
            result  = tool_fn(**merged_args)
        except TypeError as exc:
            # Raised when required positional args are missing from merged_args.
            msg = f"Tool '{tool_name}' called with wrong/missing args: {exc}"
            logger.error("Step %s failed: %s", step_num, msg)
            logs.append(_make_log_entry(step_num, tool_name, "failed", error=msg))
            return {"status": "failed", "logs": logs, "results": results}
        except Exception as exc:                        # noqa: BLE001
            # Catch-all: unexpected runtime errors inside the tool itself.
            msg = f"Tool '{tool_name}' raised an unexpected error: {exc}"
            logger.exception("Step %s failed unexpectedly.", step_num)
            logs.append(_make_log_entry(step_num, tool_name, "failed", error=msg))
            return {"status": "failed", "logs": logs, "results": results}

        # --- Persist result ---
        results.append(result)
        logs.append(_make_log_entry(step_num, tool_name, "success", result=result))
        logger.info("Step %s completed successfully: %r", step_num, result)

        # --- Update shared state with this step's output ---
        # Any key the tool returns becomes available to subsequent steps.
        if isinstance(result, dict):
            if isinstance(result, dict):
                if "file" in result:
                    execution_state["file"] = result["file"]

    return {"status": "success", "logs": logs, "results": results}