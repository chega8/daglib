from daglib.task import Task, TaskState, BashTask
from daglib.executor import DAG, DAGState, DAGExecutor
from loguru import logger

if __name__ == "__main__":

    # Example tasks
    def task_func_1(context):
        logger.debug("Task 1 is running.")
        return {"data_from_task_1": 123}
    
    def task_func_2(context):
        logger.debug(f"Task 2 is running. Received: {context.get('data_from_task_1', None)}")
        return {"data_from_task_2": "xyz"}
    
    def task_func_3(context):
        logger.debug(f"Task 3 is running. Received: {context.get('data_from_task_2', None)}")
        # Uncomment to test error
        # raise ValueError("Example error in Task 3")
        return {}

    # Build a simple DAG
    dag = DAG(dag_id="my_simple_dag")

    # Create Task instances
    t1 = Task("task1", task_func_1)
    t2 = Task("task2", task_func_2, dependencies=["task1"])
    t3 = Task("task3", task_func_3, dependencies=["task2"])

    bash_task = BashTask(
        task_id="bash_task",
        script_path="test_script.sh",
        dependencies=["task3"]
    )

    # Add tasks to the DAG
    dag.add_task(t1)
    dag.add_task(t2)
    dag.add_task(t3)
    dag.add_task(bash_task)

    # Execute the DAG
    executor = DAGExecutor()
    executor.run(dag)

    # Serialize to pickle (for potential reruns)
    dag.serialize_to_pickle(f"data/{dag.dag_id}.pkl")

    # In a new script or after a crash, we can restore DAG state
    restored_dag = DAG.deserialize_from_pickle(f"data/{dag.dag_id}.pkl")
    logger.info(f"Restored DAG State: {restored_dag.state}")
    logger.info(f"Restored Errors: {restored_dag.errors}")
