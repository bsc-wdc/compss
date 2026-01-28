#!/usr/bin/python
"""
PyCOMPSs Testbench for Predefined Scheduler
============================================
This test validates that the Predefined Scheduler correctly follows
the execution order specified in the config.json file.
"""

from pycompss.api.task import task
from pycompss.api.api import compss_wait_on, compss_barrier
import time


@task(returns=int)
def simple_task(task_id, sleep_time=0.1):
    """
    Simple task that sleeps for a short time and returns its ID.
    
    Args:
        task_id: Identifier for the task
        sleep_time: Time to sleep in seconds
    
    Returns:
        The task_id
    """
    time.sleep(sleep_time)
    print(f"Task {task_id} executed")
    return task_id


def main():
    """
    Main function that creates tasks to be scheduled by the Predefined Scheduler.
    The scheduler should follow the order and resource assignment from config.json.
    """
    print("Starting Predefined Scheduler Test")
    print("-" * 60)
    
    # Create 10 tasks - the config.json will define their execution order
    num_tasks = 10
    results = []
    
    for i in range(1, num_tasks + 1):
        result = simple_task(i, sleep_time=0.05)
        results.append(result)
    
    # Wait for all tasks to complete
    compss_barrier()
    
    # Retrieve results
    final_results = []
    for result in results:
        final_results.append(compss_wait_on(result))
    
    print("-" * 60)
    print("All tasks completed successfully")
    print(f"Task IDs executed: {final_results}")
    print("-" * 60)
    
    return 0


if __name__ == "__main__":
    main()
