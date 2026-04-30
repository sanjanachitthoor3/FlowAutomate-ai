from app.core.parser import validate_and_fix_plan

plan = [
  {'step': 1, 'tool': 'clean_data', 'args': {'file': 'data/sample.csv'}},
  {'step': 2, 'tool': 'generate_summary', 'args': {}}
]

print(validate_and_fix_plan(plan))