from app.core.planner import generate_plan
from app.core.parser import validate_and_fix_plan
from app.core.executor import execute_plan
import json


def pretty(title, data):
    print(f"\n{'='*10} {title} {'='*10}")
    print(json.dumps(data, indent=2))


# ---------------- INPUT ----------------
# prompt = "Summarize the data in data/sample.csv"
# prompt = "Summarize the data in data/sample.csv and rename it to summary.csv"
# files = ["data/sample.csv"]

##TEST 1:
# prompt = "Summarize the data in data/sample_messy.csv and rename it to messy_summary.csv"
# files = ["data/sample_messy.csv"]

##TEST 2:
# prompt = "Summarize the data in data/sample_mixed.csv and rename it to mixed_summary.csv"
# files = ["data/sample_mixed.csv"]

##TEST 3:
prompt = "Summarize the data in data/sample_clean.csv and rename it to clean_summary.csv"
files = ["data/sample_clean.csv"]

print("\n========== INPUT ==========")
print(f"Instruction: {prompt}")
print(f"Files: {files}")


# ---------------- PLANNER ----------------
plan = generate_plan(prompt, files)
pretty("GENERATED PLAN", plan)


# ---------------- PARSER ----------------
validated_plan = validate_and_fix_plan(plan)
pretty("VALIDATED PLAN", validated_plan)


# ---------------- EXECUTION ----------------
result = execute_plan(validated_plan)
pretty("EXECUTION RESULT", result)


# ---------------- FILE FLOW ----------------
print("\n========== FILE FLOW ==========")
for i, step in enumerate(result.get("results", []), start=1):
    if isinstance(step, dict) and "file" in step:
        print(f"Step {i} output file → {step['file']}")


# ---------------- FINAL OUTPUT ----------------
print("\n========== FINAL OUTPUT ==========")
if result["status"] == "success":
    final_file = result["results"][-1].get("file")
    print(f"Final file: {final_file}")
else:
    print("Execution failed.")