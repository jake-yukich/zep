from typing import List
from core.task import Task

class Executor:
    def __init__(self):
        self.running_tasks: List[Task] = []

    def execute_task(self, task: Task) -> None:
        print(f"Executing task: {task}")
        self.running_tasks.append(task)

        try:
            task.execute()
        except Exception as e:
            print(f"Error executing task {task.task_id}: {e}")
        finally:
            self.running_tasks.remove(task)

    def execute_tasks(self, tasks: List[Task]) -> None:
        for task in tasks:
            self.execute_task(task)

    def get_task_status(self, task: Task) -> str:
        if task in self.running_tasks:
            return "RUNNING"
        else:
            # TODO: check db for task status
            return ""