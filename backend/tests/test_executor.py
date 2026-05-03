from app.core.executor import execute_plan
import json

"""NORMAL FLOW TEST"""
# plan = [
#   {'step': 1, 'tool': 'clean_data', 'args': {'file': 'data/sample.csv'}},
#   {'step': 2, 'tool': 'generate_summary', 'args': {'file': 'data/sample.csv'}}
# ]

"""INVALID TOOL TEST"""
# plan = [
#   {'step': 1, 'tool': 'fake_tool', 'args': {}}
# ]

"""MISSING ARGS TEST"""
plan = [
  {'step': 1, 'tool': 'generate_summary', 'args': {}}
]
result= execute_plan(plan)
print(json.dumps(result, indent=2))