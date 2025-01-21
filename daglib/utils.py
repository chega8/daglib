import requests

def render_dag_status(dag) -> str:
    """
    Returns a multi-line string with colorful DAG status:
    - Each task
    - Its dependencies
    - Current state (with color)
    """
    # ANSI color codes (simplified example)
    color_map = {
        "PENDING": "\033[93m",  # Yellow
        "RUNNING": "\033[94m",  # Blue
        "SUCCESS": "\033[92m",  # Green
        "FAILED":  "\033[91m",  # Red
        "RESET":   "\033[0m"    # Reset to default
    }
    
    lines = []
    lines.append("\n=== DAG STATUS OVERVIEW ===")
    lines.append(f"DAG: {dag.dag_id}")
    lines.append(f"Overall DAG State: {color_map.get(dag.state.name, '')}{dag.state.name}{color_map['RESET']}")
    lines.append("")

    for task_id, task in sorted(dag.tasks.items(), key=lambda x: x[0]):
        deps = ", ".join(task.dependencies) if task.dependencies else "(none)"
        color = color_map.get(task.state.name, "")
        reset = color_map["RESET"]
        lines.append(
            f"  {task_id} <- [{deps}] | state: {color}{task.state.name}{reset}"
        )
    lines.append("============================\n")
    
    return "\n".join(lines)

def parse_job_name(txt_logs):
    import subprocess
    import re

    txt_logs = txt_logs.strip()
    pattern = r"lm-mpi-job-\b[\w-]+\b"

    lines = txt_logs.splitlines()
    if lines:
        last_line = lines[-1]
        match = re.search(pattern, last_line)
        if match:
            job_id = match.group(0)
            return job_id
        else:
            print("No job ID found in the last line.")
            return None
    else:
        print("No output from the command.")
        return None


def check_job_status(job_name, region='SR006'):
    try:
        import client_lib
    except ImportError:
        print("Client library not found.")
        return
    
    r = requests.get(
        f"http://{client_lib.environment.GW_API_ADDR}/job_status",
        params={"job": job_name, "region": region},
        headers={"X-Api-Key": client_lib.environment.GW_API_KEY, "X-Namespace": client_lib.environment.NAMESPACE},
    )
    if r.status_code == 200:
        print(f'Job status={r.json().get("job_status")}')
    else:
        return f"Cant get status for job, check job_name"