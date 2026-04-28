import os
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field, field_validator
from typing import Optional

router = APIRouter()


# --- Request / Response Models ---

class WorkflowRequest(BaseModel):
    instruction: str
    files: Optional[list[str]] = Field(default_factory=list)

    @field_validator("instruction")
    @classmethod
    def instruction_must_not_be_empty(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("instruction must not be empty")
        return v.strip()


class WorkflowResponse(BaseModel):
    instruction: str
    files: list[str]


# --- Endpoint ---

@router.post("/run-workflow", response_model=WorkflowResponse)
def run_workflow(payload: WorkflowRequest) -> WorkflowResponse:
    """
    Module 1 — Input Handler

    Accepts a user instruction and optional file paths, validates them,
    and returns a structured payload ready for the LLM Planner.
    """
    files = payload.files or []

    # Validate file existence
    missing = [f for f in files if not os.path.exists(f)]
    if missing:
        raise HTTPException(
            status_code=400,
            detail=f"File(s) not found: {', '.join(missing)}"
        )

    return WorkflowResponse(
        instruction=payload.instruction,
        files=files,
    )
