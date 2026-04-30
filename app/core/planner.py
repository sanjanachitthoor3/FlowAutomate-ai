from dotenv import load_dotenv
load_dotenv()
import os
import json
import logging
from groq import Groq

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Supported tools the planner is allowed to emit — enforced via the prompt.
ALLOWED_TOOLS = ["clean_data", "rename_files", "generate_summary"]

# Model to use. Swap for any Groq-hosted model as needed.
GROQ_MODEL = "llama-3.1-8b-instant"


# ---------------------------------------------------------------------------
# Prompt construction
# ---------------------------------------------------------------------------

def _build_prompt(instruction: str, files: list[str]) -> str:
    """
    Build a tightly-constrained system + user prompt pair (returned as a
    single user message here; the system role is handled in the API call).

    The prompt does three things:
      1. Restricts the model to a fixed tool vocabulary.
      2. Mandates strict JSON-only output (no prose, no markdown fences).
      3. Injects the user instruction and available files as context.
    """
    files_context = ", ".join(files) if files else "none"

    return (
        f"You are a workflow planning engine. "
        f"Your ONLY job is to convert a natural language instruction into a "
        f"structured JSON plan.\n\n"
        f"RULES:\n"
        f"1. You MUST respond with a JSON array and nothing else — no explanation, "
        f"   no markdown, no code fences.\n"
        f"2. Each element in the array is a step object with exactly these keys:\n"
        f'   {{ "step": <int>, "tool": "<tool_name>", "args": {{<key>: <value>}} }}\n'
        f"3. You may ONLY use these tools: {', '.join(ALLOWED_TOOLS)}.\n"
        f"4. Never invent tools outside the allowed list.\n\n"
        f"Available files: {files_context}\n"
        f"Instruction: {instruction}\n\n"
        f"Respond with the JSON plan now:"
    )


# ---------------------------------------------------------------------------
# LLM call
# ---------------------------------------------------------------------------

def _call_llm(prompt: str) -> str:
    """
    Send the prompt to the Groq API and return the raw text response.

    Raises:
        RuntimeError: if the API key is missing or the request fails.
    """
    api_key = os.getenv("GROQ_API_KEY")
    if not api_key:
        raise RuntimeError(
            "GROQ_API_KEY environment variable is not set. "
            "Export it before running the planner."
        )

    client = Groq(api_key=api_key)

    response = client.chat.completions.create(
        model=GROQ_MODEL,
        messages=[
            # System role: reinforces JSON-only behaviour at the model level.
            {
                "role": "system",
                "content": (
                    "You are a strict JSON workflow planner. "
                    "Output only valid JSON arrays. No prose. No markdown."
                ),
            },
            {"role": "user", "content": prompt},
        ],
        temperature=0,        # Deterministic — plans should not vary randomly.
        max_tokens=1024,
    )

    # Extract the text content from the first choice.
    return response.choices[0].message.content.strip()


# ---------------------------------------------------------------------------
# Response parsing
# ---------------------------------------------------------------------------

def _parse_plan(raw: str) -> list:
    """
    Parse the LLM's raw text into a validated Python list.

    Strategy:
      - Strip accidental markdown fences (``` or ```json) that the model
        might emit despite instructions.
      - Attempt JSON parsing.
      - Validate that the result is a non-empty list of step dicts.
      - Filter out any steps whose tool is not in ALLOWED_TOOLS.

    Returns an empty list on any unrecoverable parse failure so the caller
    can decide how to handle it (fail fast, retry, etc.).
    """
    # Remove markdown code fences if present.
    cleaned = raw.strip().removeprefix("```json").removeprefix("```").removesuffix("```").strip()

    try:
        plan = json.loads(cleaned)
    except json.JSONDecodeError as exc:
        logger.error("LLM returned invalid JSON: %s | Raw response: %r", exc, raw)
        return []

    # Ensure the top-level structure is a list.
    if not isinstance(plan, list):
        logger.error("Expected a JSON array, got %s. Raw: %r", type(plan).__name__, raw)
        return []

    validated_steps = []
    for item in plan:
        # Each step must be a dict with the required keys.
        if not isinstance(item, dict):
            logger.warning("Skipping non-dict step: %r", item)
            continue

        missing_keys = {"step", "tool", "args"} - item.keys()
        if missing_keys:
            logger.warning("Skipping step missing keys %s: %r", missing_keys, item)
            continue

        # Reject any tool not in the allowed list.
        if item["tool"] not in ALLOWED_TOOLS:
            logger.warning("Skipping step with unknown tool %r: %r", item["tool"], item)
            continue

        validated_steps.append(item)

    return validated_steps


# ---------------------------------------------------------------------------
# Public interface
# ---------------------------------------------------------------------------

def generate_plan(instruction: str, files: list[str]) -> list:
    """
    Convert a natural language instruction into a structured workflow plan.

    Args:
        instruction: The user's plain-English task description.
        files:       Paths to any files the user has provided.

    Returns:
        A list of step dicts, e.g.:
        [
            {"step": 1, "tool": "clean_data",       "args": {"file": "data.csv"}},
            {"step": 2, "tool": "generate_summary",  "args": {"file": "data.csv"}},
        ]
        Returns an empty list if planning fails.

    Raises:
        RuntimeError: if the LLM API call itself fails (missing key, network, etc.).
    """
    # 1. Build the prompt from the user's instruction and file list.
    prompt = _build_prompt(instruction, files)
    logger.debug("Planner prompt:\n%s", prompt)

    # 2. Call the LLM and get raw text back.
    raw_response = _call_llm(prompt)
    logger.debug("Raw LLM response: %r", raw_response)

    # 3. Parse and validate the response into a clean Python list.
    plan = _parse_plan(raw_response)
    logger.info("Generated plan with %d step(s).", len(plan))

    return plan
