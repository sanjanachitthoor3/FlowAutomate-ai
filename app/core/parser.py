import logging
from typing import Any

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Tools that the execution engine knows how to run.
# Any step referencing a tool outside this set is rejected.
ALLOWED_TOOLS = {"clean_data", "rename_files", "generate_summary"}

# Tools that require a "file" argument to function correctly.
# Used by the auto-fix logic to propagate the file from a prior step.
FILE_REQUIRED_TOOLS = {"clean_data", "rename_files", "generate_summary"}


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _is_valid_structure(step: Any) -> bool:
    """
    Return True only if `step` is a dict that contains all three
    mandatory keys: 'step', 'tool', and 'args'.

    This is a structural check — it does not inspect the values yet.
    """
    if not isinstance(step, dict):
        logger.warning("Rejected non-dict step: %r", step)
        return False

    missing = {"step", "tool", "args"} - step.keys()
    if missing:
        logger.warning("Rejected step missing keys %s: %r", missing, step)
        return False
    if not isinstance(step["args"], dict):
        logger.warning("Rejected step with invalid args (not dict): %r", step["args"])
        return False
    return True


def _is_allowed_tool(step: dict) -> bool:
    """
    Return True if the step's tool is in the approved list.

    Rejects hallucinated or misspelled tool names produced by the LLM.
    """
    if step["tool"] not in ALLOWED_TOOLS:
        logger.warning(
            "Rejected step %s — unknown tool %r. Allowed: %s",
            step.get("step"), step["tool"], ALLOWED_TOOLS,
        )
        return False
    return True


def _fix_missing_file(step: dict, last_known_file: str | None) -> dict:
    """
    If the step's tool requires a 'file' arg and none is present,
    attempt to inherit the file path from the previous valid step.

    Args:
        step:            The step dict to inspect and potentially fix.
        last_known_file: The most recent 'file' value seen in prior steps,
                         or None if no file has appeared yet.

    Returns:
        The (possibly mutated) step dict.
    """
    tool = step["tool"]
    args = step["args"]

    # Only attempt a fix for tools that actually need a file.
    if tool not in FILE_REQUIRED_TOOLS:
        return step

    if "file" not in args or not args["file"]:
        if last_known_file:
            # Propagate the file from the closest preceding step that had one.
            logger.info(
                "Step %s: 'file' arg missing for tool %r — "
                "inheriting %r from previous step.",
                step["step"], tool, last_known_file,
            )
            step["args"] = {**args, "file": last_known_file}
        else:
            # No prior file to fall back on; log and leave as-is.
            # The execution engine will surface this as a runtime error
            # rather than silently doing the wrong thing.
            logger.warning(
                "Step %s: 'file' arg missing for tool %r and no prior "
                "file is available to inherit.",
                step["step"], tool,
            )

    return step


# ---------------------------------------------------------------------------
# Public interface
# ---------------------------------------------------------------------------


## ENTRY POINT OF THIS MODULE
def validate_and_fix_plan(plan: list) -> list:
    """
    Validate and auto-correct a workflow plan produced by the LLM Planner.

    Processing rules (applied in order for every step):
      1. Skip non-dict entries entirely.
      2. Skip steps that are missing any of: 'step', 'tool', 'args'.
      3. Skip steps whose 'tool' is not in ALLOWED_TOOLS.
      4. If a valid step is missing a required 'file' arg, inherit the
         file path from the most recent step that provided one.
      5. Renumber 'step' sequentially so the output index is always
         contiguous (1, 2, 3 …) regardless of what the LLM emitted.

    Args:
        plan: Raw list of step dicts from the LLM Planner.

    Returns:
        A cleaned, validated, and renumbered list of step dicts ready
        for the Execution Engine. May be shorter than the input if
        invalid steps were dropped.
    """
    if not isinstance(plan, list):
        logger.error("validate_and_fix_plan received a non-list: %r", plan)
        return []

    validated: list[dict] = []

    # Tracks the most recently seen 'file' value across steps so we can
    # propagate it to subsequent steps that omit it.
    last_known_file: str | None = None

    for raw_step in plan:

        # --- Gate 1: structural check ---
        if not _is_valid_structure(raw_step):
            continue  # Drop and move on; already logged inside helper.

        # Work on a shallow copy so we never mutate the caller's input.
        step = dict(raw_step)
        step["args"] = dict(raw_step.get("args") or {})

        # --- Gate 2: allowed-tool check ---
        if not _is_allowed_tool(step):
            continue  # Drop unknown/hallucinated tools.

        # --- Fix: propagate 'file' if missing ---
        step = _fix_missing_file(step, last_known_file)

        # Update the running file tracker for future steps to inherit from.
        if step["args"].get("file"):
            last_known_file = step["args"]["file"]

        validated.append(step)

    # --- Renumber steps sequentially (1-based) ---
    # The LLM may have emitted duplicate numbers, gaps, or wrong values.
    for new_index, step in enumerate(validated, start=1):
        step["step"] = new_index

    logger.info(
        "Parser complete: %d valid step(s) from %d input step(s).",
        len(validated), len(plan),
    )

    return validated