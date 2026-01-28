import re
import json
import sys
import os


def parse_logs(log_file_path):
    """
    Genera una lista de tareas a partir de un runtime.log de COMPSs.
    
    Los predecesores se calculan basándose en el ORDEN de asignación en cada recurso.
    Si en un recurso las tareas se asignan en orden: Task1, Task2, Task3
    entonces Task2 tiene predecesor [1], Task3 tiene predecesor [2].
    """

    tasks = {}  # taskId -> dict
    resource_task_order = {}  # resource -> [taskId1, taskId2, ...]

    # Patrón para asignación de tareas
    # Formato: [(787)(2025-12-10 15:32:42,448) TaskScheduler] ... Assigning action ... (Task 1, ...) to worker COMPSsWorker01 with implementation 0
    assign_re = re.compile(
        r"\[\(\d+\)\([^\)]+\)\s+\S+\]\s+\S+\s+-\s+Assigning action \S+ \(Task (\d+),[^\)]+\) to worker (\S+) with implementation (\d+)"
    )

    def ensure_task(tid: int):
        """Crea estructura por defecto para una tarea si no existe."""
        if tid not in tasks:
            tasks[tid] = {
                "taskId": tid,
                "implementationId": 0,
                "predecessors": [],
                "_resources": [],
            }
        return tasks[tid]

    # Leer el log y extraer asignaciones
    with open(log_file_path, "r") as f:
        for line in f:
            m = assign_re.search(line)
            if m:
                tid = int(m.group(1))
                worker = m.group(2)
                impl = int(m.group(3))
                
                t = ensure_task(tid)
                
                # Añadir recurso si no está
                if worker not in t["_resources"]:
                    t["_resources"].append(worker)
                
                # Actualizar implementationId
                if t["implementationId"] == 0:
                    t["implementationId"] = impl
                
                # Registrar orden de asignación por recurso
                if worker not in resource_task_order:
                    resource_task_order[worker] = []
                
                # Solo añadir si no está ya en la lista (evitar duplicados)
                if tid not in resource_task_order[worker]:
                    resource_task_order[worker].append(tid)

    # Calcular predecesores basándose en el orden de asignación
    for resource, task_list in resource_task_order.items():
        # Para cada tarea en este recurso (excepto la primera)
        for i in range(1, len(task_list)):
            current_task_id = task_list[i]
            previous_task_id = task_list[i - 1]
            
            # El predecesor es la tarea inmediatamente anterior en este recurso
            t = tasks[current_task_id]
            if previous_task_id not in t["predecessors"]:
                t["predecessors"].append(previous_task_id)

    # Post-procesado: decidir resource vs resources
    result = []
    for tid in sorted(tasks.keys()):
        t = tasks[tid]
        resources = t.pop("_resources", [])

        if not resources:
            t["resource"] = "UNKNOWN_RESOURCE"
        elif len(resources) == 1:
            t["resource"] = resources[0]
        else:
            # Tarea multinodo: usamos "resources" como lista
            t["resources"] = resources

        result.append(t)

    return result


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 generate_config.py <runtime.log>")
        sys.exit(1)

    log_file = sys.argv[1]
    if not os.path.exists(log_file):
        print(f"Error: File {log_file} not found.")
        sys.exit(1)

    data = parse_logs(log_file)
    print(json.dumps(data, indent=2))
