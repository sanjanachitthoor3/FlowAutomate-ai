#llm must convert user input into workflow steps

from app.core.planner import generate_plan

plan = generate_plan(
    "Clean data.csv and generate summary",
    ["data/sample.csv"]
)

print(plan)