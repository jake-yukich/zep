from typing import Optional, Callable
from __future__ import annotations

class Task:
    def __init__(
        self,
        task_id: str,
        python_callable: Callable,
        dag: Optional[DAG] = None,
    ):
        self.task_id = task_id
        self.python_callable = python_callable
        self.dag = dag

    def set_upstream(self, other: Task) -> None:
        if self.dag is None or other.dag is None:
            raise ValueError("Both tasks must be added to a DAG before setting dependencies")
        self.dag.set_dependency(other.task_id, self.task_id)

    def set_downstream(self, other: Task) -> None:
        if self.dag is None or other.dag is None:
            raise ValueError("Both tasks must be added to a DAG before setting dependencies")
        self.dag.set_dependency(self.task_id, other.task_id)

    def execute(self) -> None:
        self.python_callable()

    def __rshift__(self, other: Task) -> Task:
        self.set_downstream(other)
        return other

    def __lshift__(self, other: Task) -> Task:
        self.set_upstream(other)
        return other

    def __repr__(self) -> str:
        return f"<Task: {self.task_id}>"

from core.dag import DAG