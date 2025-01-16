
import json
import pickle
from enum import Enum
from typing import Any, Callable, Dict, List, Optional
from loguru import logger
import subprocess

class TaskState(Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"

class Task:
    """
    Represents a single task in the DAG.
    `task_id`: Unique identifier for the task
    `func`: A callable to be run for this task
    `dependencies`: List of task_ids that must finish before this task starts
    """
    def __init__(self, task_id: str, func: Callable, dependencies: Optional[List[str]] = None):
        self.task_id = task_id
        self.func = func
        self.dependencies = dependencies if dependencies else []
        
        self.state = TaskState.PENDING
        self.result: Any = None  # Store success result or error info

    def run(self, context: Dict[str, Any]) -> None:
        """
        Execute the task's callable with the current context.
        The `context` can store data that tasks share among themselves.
        """
        logger.info(f"Task '{self.task_id}' starting. Current state: {self.state.value}")
        self.state = TaskState.RUNNING
        
        try:
            updated_data = self.func(context)
            if isinstance(updated_data, dict):
                context.update(updated_data)
            
            self.state = TaskState.SUCCESS
            self.result = "Success"
            logger.info(f"Task '{self.task_id}' completed successfully.")
        except Exception as e:
            self.state = TaskState.FAILED
            self.result = f"Error: {str(e)}"
            logger.error(f"Task '{self.task_id}' failed with error: {e}")

    def serialize(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "status": self.status.value,
            "error": self.error,
            "result": self.result,
        }

    @staticmethod
    def deserialize(data: Dict[str, Any], func: Callable[..., Any]):
        task = Task(data["name"], func)
        task.id = data["id"]
        task.status = TaskState(data["status"])
        task.error = data.get("error")
        task.result = data.get("result")
        return task
    
    def __repr__(self):
        return f"<Task {self.task_id} (state={self.state})>"


class BashTask(Task):
    """
    A specialized Task that runs a Bash script with given arguments.
    Success is determined by a zero exit code; non-zero means failure.
    """
    def __init__(
        self, 
        task_id: str, 
        script_path: str, 
        script_args: Optional[List[str]] = None, 
        dependencies: Optional[List[str]] = None
    ):
        # Instead of passing a user-defined function, we'll use an internal callable
        # that runs the Bash script and checks the return code.
        super().__init__(
            task_id=task_id, 
            func=self._run_script, 
            dependencies=dependencies
        )
        self.script_path = script_path
        self.script_args = script_args if script_args else []

    def _run_script(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Executes the provided Bash script using subprocess.
        Raises an exception if the return code is non-zero.
        """
        logger.info(f"Executing Bash script: {self.script_path} with args: {self.script_args}")
        
        # Run the script in a subprocess (blocking call here).
        # For truly "background" execution while continuing, you could use Popen
        # and track the process handle, but for DAG orchestration, you typically
        # want to wait until the script finishes to proceed to the next task.
        proc = subprocess.run(
            [self.script_path, *self.script_args],
            capture_output=True,
            text=True
        )

        # Check if the script executed successfully
        if proc.returncode == 0:
            logger.info(f"Bash script '{self.script_path}' completed with exit code 0.")
            return {}
        else:
            error = f"Script '{self.script_path}' returned non-zero exit code: {proc.returncode}"
            error += f"\nStderr: {proc.stderr}"
            logger.error(error)
            self.result = error
            # Raise an exception to mark this task as FAILED
            raise RuntimeError(
                f"Script '{self.script_path}' returned non-zero exit code: {proc.returncode}"
            )