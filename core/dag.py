from datetime import datetime, timedelta
from typing import Dict, List, Optional
from __future__ import annotations

class DAG:
    """
    A Directed Acyclic Graph (DAG) representing a workflow of tasks.
    """
    def __init__(
        self,
        dag_id: str,
        description: str = "",
        start_date: datetime = datetime.now(),
        schedule_interval: Optional[timedelta] = None,
    ):
        self.dag_id = dag_id
        self.description = description
        self.start_date = start_date
        self.schedule_interval = schedule_interval
        self.tasks: Dict[str, Task] = {}
        self.task_dependencies: Dict[str, List[str]] = {}

    def add_task(self, task: Task) -> None:
        self.tasks[task.task_id] = task
        self.task_dependencies[task.task_id] = []
        task.dag = self

    def set_dependency(self, upstream_task_id: str, downstream_task_id: str) -> None:
        if upstream_task_id not in self.tasks or downstream_task_id not in self.tasks:
            raise ValueError("Both tasks must be added to the DAG before setting dependencies")
        
        self.task_dependencies[downstream_task_id].append(upstream_task_id)

    def get_task(self, task_id: str) -> Task:
        return self.tasks[task_id]

    def topological_sort(self) -> List[Task]:
        visited = set()
        sorted_tasks = []

        def dfs(task_id):
            visited.add(task_id)
            for upstream_id in self.task_dependencies[task_id]:
                if upstream_id not in visited:
                    dfs(upstream_id)
            sorted_tasks.append(self.tasks[task_id])

        for task_id in self.tasks:
            if task_id not in visited:
                dfs(task_id)

        return list(reversed(sorted_tasks))

    def get_task_dependencies(self) -> Dict[str, List[str]]:
        return self.task_dependencies

    def __repr__(self) -> str:
        return f"<DAG: {self.dag_id}>"

from core.task import Task