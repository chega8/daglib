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

    # Sort tasks by name just for a consistent order
    for task_id, task in sorted(dag.tasks.items(), key=lambda x: x[0]):
        # Dependencies as a comma-separated list
        deps = ", ".join(task.dependencies) if task.dependencies else "(none)"
        # Colorize the state
        color = color_map.get(task.state.name, "")
        reset = color_map["RESET"]
        lines.append(
            f"  {task_id} -> [{deps}] | state: {color}{task.state.name}{reset}"
        )
    lines.append("============================\n")
    
    return "\n".join(lines)
