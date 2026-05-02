import logging
from typing import Any, Callable
from app.tools.clean_data import clean_data
from app.tools.rename_files import rename_files
from app.tools.generate_summary import generate_summary
from app.tools.registry import TOOL_REGISTRY

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

#new helper needed for updated execution engine which allows error modes
def _try_run_tool(tool_name: str, merged_args: dict) -> tuple[Any, str]:
    """
    Call a tool from TOOL_REGISTRY and return (result, error_message).

    Returns (result, "")      on success.
    Returns (None, error_msg) on failure.

    Separating execution from error-mode decisions keeps execute_plan readable
    and avoids copy-pasting the try/except block for the retry path.
    """
    try:
        result = TOOL_REGISTRY[tool_name](**merged_args)
        return result, ""
    except TypeError as exc:
        # Missing or wrong arguments — typically a planner/parser bug.
        return None, f"Tool '{tool_name}' called with wrong/missing args: {exc}"
    except Exception as exc:            # noqa: BLE001
        # Unexpected runtime error inside the tool itself.
        logger.exception("Unexpected error in tool %r.", tool_name)
        return None, f"Tool '{tool_name}' raised an unexpected error: {exc}"
    

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
    merged = dict(args)

    # Override file ONLY if it exists in state
    if "file" in state:
        merged["file"] = state["file"]
        
    return merged


# ---------------------------------------------------------------------------
# Public interface
# ---------------------------------------------------------------------------

def execute_plan(plan: list, error_mode: str = "fail") -> dict:
    """
    Execute a validated workflow plan step-by-step.

    For each step the engine:
      1. Looks up the tool function in TOOL_REGISTRY.
      2. Merges accumulated execution state into the step's args so
         outputs from earlier steps are available to later ones.
      3. Calls the tool and stores its result.
      4. Appends a structured log entry.
      5. Handles failures according to `error_mode`.

    Args:
        plan:       A validated list of step dicts from the Plan Parser.
                    Each dict must have: "step" (int), "tool" (str), "args" (dict).
        error_mode: How to handle a failing step. One of:
                      "fail"  — stop immediately on the first error (default).
                      "skip"  — log the error and continue to the next step.
                      "retry" — retry the failing step once; stop if it fails again.

    Returns:
        {
            "status":  "success" | "failed",
            "logs":    [ { step, tool, status, result, error }, ... ],
            "results": [ <tool return value>, ... ]   # only successful steps
        }
    """
    # Guard: reject unknown modes early so the caller gets a clear error.
    if error_mode not in ("fail", "skip", "retry"):
        raise ValueError(
            f"Invalid error_mode {error_mode!r}. Must be 'fail', 'skip', or 'retry'."
        )

    logs:    list[dict] = []
    results: list[Any]  = []

    # Shared state bucket: populated with each tool's output so subsequent
    # steps can reference values produced by earlier ones (e.g. "file").
    execution_state: dict = {}

    for step_def in plan:
        step_num  = step_def.get("step", "?")
        tool_name = step_def.get("tool", "")
        raw_args  = dict(step_def.get("args") or {})

        logger.info("Executing step %s — tool: %r", step_num, tool_name)

        # --- Gate: tool must exist in registry ---
        # This is a plan-level error (bad tool name), not a runtime error,
        # so it always hard-stops regardless of error_mode.
        if tool_name not in TOOL_REGISTRY:
            msg = f"Unknown tool '{tool_name}' — not found in TOOL_REGISTRY."
            logger.error("Step %s failed: %s", step_num, msg)
            logs.append(_make_log_entry(step_num, tool_name, "failed", error=msg))
            return {"status": "failed", "logs": logs, "results": results}

        # --- Merge state into args (state fills gaps, args always win) ---
        merged_args = _merge_state(raw_args, execution_state)

        # --- Execute (with error_mode-aware handling) ---
        result, error_msg = _try_run_tool(tool_name, merged_args)

        if error_msg and error_mode == "retry":
            # --- RETRY: attempt the step one more time before giving up ---
            logger.warning("Step %s failed — retrying once. Error: %s", step_num, error_msg)
            result, error_msg = _try_run_tool(tool_name, merged_args)

        if error_msg:
            # The step failed (either on first attempt, or after a retry).
            logs.append(_make_log_entry(step_num, tool_name, "failed", error=error_msg))

            if error_mode == "skip":
                # --- SKIP: log and move on; do not update execution state ---
                logger.warning("Step %s skipped after error: %s", step_num, error_msg)
                continue

            # --- FAIL (default) or exhausted RETRY: stop execution ---
            logger.error("Step %s failed: %s", step_num, error_msg)
            return {"status": "failed", "logs": logs, "results": results}

        # --- Persist result ---
        results.append(result)
        logs.append(_make_log_entry(step_num, tool_name, "success", result=result))
        logger.info("Step %s completed successfully: %r", step_num, result)

        # --- Update shared state with this step's output ---
        # Any key the tool returns becomes available to subsequent steps.
        if isinstance(result, dict) and "file" in result:
            execution_state["file"] = result["file"]

    return {"status": "success", "logs": logs, "results": results}