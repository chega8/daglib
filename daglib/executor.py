import json
from daglib.utils import render_dag_status
import dill
from enum import Enum
from typing import Any, Callable, Dict, List, Optional
from loguru import logger

from daglib.task import Task, TaskState
import datetime

log_name = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M")

logger.add(
    f"logs/{log_name}.log",
    format="{time} {level} {message}",
    rotation="10 MB",     
    compression="zip"     
)


class DAGState(Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"

class DAG:
    """
    Represents a Directed Acyclic Graph (DAG) of tasks.
    Each task is keyed by its task_id in `tasks`.
    """
    def __init__(self, dag_id: str):
        self.dag_id = dag_id
        self.tasks: Dict[str, Task] = {}
        
        self.state = DAGState.PENDING
        self.errors: Dict[str, str] = {}  # Keep track of errors by task_id

    def add_task(self, task: Task) -> None:
        logger.debug(f"Adding task '{task.task_id}' to DAG '{self.dag_id}'.")
        self.tasks[task.task_id] = task

    def get_task(self, task_id: str) -> Optional[Task]:
        return self.tasks.get(task_id, None)

    def serialize_to_json(self, path: str) -> None:
        """
        Serialize the DAG to JSON (only storing minimal state).
        """
        logger.info(f"Serializing DAG '{self.dag_id}' to JSON at: {path}")
        data = {
            "dag_id": self.dag_id,
            "tasks": [task.serialize() for task in self.tasks],
            "dag_state": self.state.value
        }
        with open(path, "w") as f:
            json.dump(data, f, indent=2)

    def serialize_to_pickle(self, path: str) -> None:
        """
        Serialize the entire DAG object via pickle.
        """
        logger.info(f"Serializing DAG '{self.dag_id}' to pickle at: {path}")
        with open(path, "wb") as f:
            dill.dump(self, f)

    @staticmethod
    def deserialize_from_pickle(path: str) -> "DAG":
        logger.info(f"Deserializing DAG from pickle at: {path}")
        with open(path, "rb") as f:
            dag: DAG = dill.load(f)
        return dag

    def __repr__(self):
        return f"<DAG {self.dag_id} (state={self.state})>"

            
class DAGExecutor:
    """
    Executes the tasks in a DAG sequentially (or in a simplified dependency-aware order).
    """
    def __init__(self):
        pass

    def run(self, dag: DAG, context: Optional[Dict[str, Any]] = None, rerun: bool = False) -> None:
        """
        - Parse the DAG
        - Resolve dependencies and run tasks in order
        - Update DAG states and handle errors
        """
        logger.info(f"Starting execution of DAG '{dag.dag_id}'")
        
        if context is None:
            context = {}

        # Initialize DAG state
        dag.state = DAGState.RUNNING
        
        remaining_tasks = set(dag.tasks.keys())
        
        while remaining_tasks:
            made_progress = False
            
            for task_id in list(remaining_tasks):
                task = dag.get_task(task_id)
                
                if task.state == TaskState.SUCCESS and rerun:
                    remaining_tasks.remove(task_id)
                    continue
                
                if all(dag.get_task(dep).state == TaskState.SUCCESS for dep in task.dependencies):
                    task.run(context)

                    if task.state == TaskState.FAILED:
                        dag.errors[task_id] = task.result
                        logger.error(f"Task '{task_id}' caused DAG '{dag.dag_id}' to fail.")
                        dag.state = DAGState.FAILED
                    
                    remaining_tasks.remove(task_id)
                    made_progress = True
            
            if not made_progress and remaining_tasks:
                logger.error(
                    f"No progress made for DAG '{dag.dag_id}', "
                    f"remaining tasks: {remaining_tasks}."
                )
                dag.state = DAGState.FAILED
                break

        if dag.state != DAGState.FAILED:
            dag.state = DAGState.COMPLETED
            logger.info(f"DAG '{dag.dag_id}' execution completed successfully.")
        else:
            logger.warning(f"DAG '{dag.dag_id}' ended in FAILED state.")
            
        print(render_dag_status(dag))