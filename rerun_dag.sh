#!/bin/bash
export PYTHONPATH="${PYTHONPATH}:."
nohup python3.11 dags/test_dag2.py --rerun > logs/dag_run.log 2>&1 &