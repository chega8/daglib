#!/bin/bash
export PYTHONPATH="${PYTHONPATH}:."
nohup python3.11 dags/test_dag.py > logs/dag_run.log 2>&1 &