""""  Module 1 — Input Handler -API +VALIDATION LAYER

    Accepts a user instruction and optional file paths, validates them,
    and returns a structured payload ready for the LLM Planner.

    Example prompt: "Clean data.csv and generate summary, pls check thoroughly..."
    this module validates the request, and converts the prompt into a structured format like:
    {
        "instruction": "Clean data.csv and generate summary, pls check thoroughly...",
        "files": ["data.csv"]
    }
    
    """


import os
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field, field_validator
from typing import Optional
from app.core.planner import generate_plan
from app.core.parser import validate_and_fix_plan
from app.core.executor import execute_plan


router = APIRouter()


# --- Request / Response Models ---

class WorkflowRequest(BaseModel):
    instruction: str
    files: Optional[list[str]] = Field(default_factory=list)
    preview: bool = False

    #runs automatically when req is received, checks if instruction is empty and raises error if it is
    @field_validator("instruction")
    @classmethod
    def instruction_must_not_be_empty(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("instruction must not be empty")
        return v.strip()


#defines response model
class WorkflowResponse(BaseModel):
    instruction: str
    files: list[str]


# --- Endpoint ---

@router.post("/run-workflow")
def run_workflow(payload: WorkflowRequest): 
    files = payload.files or []

    # Validate file existence
    missing = [f for f in files if not os.path.exists(f)]
    if missing:
        raise HTTPException(
            status_code=400,
            detail=f"File(s) not found: {', '.join(missing)}"
        )

    plan = generate_plan(payload.instruction, files)
    validated_plan = validate_and_fix_plan(plan)

    # --- Preview mode ---
    if payload.preview:
        return {
            "mode": "preview",
            "plan": plan,
            "validated_plan": validated_plan
        }

    # --- Execute mode ---
    result = execute_plan(validated_plan)

    return {
        "mode": "execute",
        "plan": plan,
        "validated_plan": validated_plan,
        "result": result
    }