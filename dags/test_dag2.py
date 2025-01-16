import argparse
from daglib.task import Task, TaskState, BashTask
from daglib.executor import DAG, DAGState, DAGExecutor
from loguru import logger

import random
import os


def task_func_1(context):
    logger.debug("Task 1 is running. Setting 'data_from_task_1' in context.")
    context["data_from_task_1"] = 42

def task_func_2(context):
    logger.debug("Task 2 is running. 50% chance of failing randomly.")
    if random.random() < 0.5:
        raise ValueError("Random 50% failure in task2.")
    context["data_from_task_2"] = "Hello from task2"

def task_func_3(context):
    logger.debug(f"Task 3 is running. Received data_from_task_2={context.get('data_from_task_2')}")


if __name__ == "__main__":
    argparser = argparse.ArgumentParser()
    argparser.add_argument("--rerun", default=False, action='store_true')
    args = argparser.parse_args()
    
    dag_pickle_path = "my_dag_state.pkl"

    # Build a fresh DAG
    dag = DAG(dag_id="random_failure_dag")

    t1 = Task("task1", task_func_1)
    t2 = Task("task2", task_func_2, dependencies=["task1"])
    t3 = Task("task3", task_func_3, dependencies=["task2"])

    dag.add_task(t1)
    dag.add_task(t2)
    dag.add_task(t3)

    executor = DAGExecutor()

    max_retries = 5
    attempt = 1
    
    if args.rerun:
        if os.path.exists(dag_pickle_path):
            logger.info("Restoring DAG state from pickle.")
            dag = DAG.deserialize_from_pickle(dag_pickle_path)
            executor.run(dag, rerun=args.rerun)
        else:
            logger.info("Rerun flag set, but no pickle found. Starting fresh.")
            args.rerun = False
    else:
        executor.run(dag)
        dag.serialize_to_pickle(dag_pickle_path)

    # while attempt <= max_retries:
    #     logger.info(f"===== DAG Attempt #{attempt} =====")

        
    #     if attempt == 1:
    #         executor.run(dag)
    #     else:
    #         dag = DAG.deserialize_from_pickle(dag_pickle_path)
    #         executor.run(dag, rerun=True)

    #     if dag.state == DAGState.COMPLETED:
    #         logger.info("DAG completed successfully! Exiting loop.")
    #         break
    #     else:
    #         logger.info("DAG failed. Serializing state for another attempt.")
    #         dag.serialize_to_pickle(dag_pickle_path)
    #         attempt += 1

    # if os.path.exists(dag_pickle_path):
    #     os.remove(dag_pickle_path)
