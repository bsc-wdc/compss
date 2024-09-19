"""
Utils Module for reproducibility_methods module

"""
import os
import subprocess

from rocrate.rocrate import ROCrate

def get_file_names(folder_path: str) -> dict:
    """
    Get the file names in the folder path
    """
    file_names = {}
    for root,_,files in os.walk(folder_path):
        for file in files:
            file_names[file] = os.path.join(root, file)
    return file_names


def get_Create_Action(entity:ROCrate):
    """
    Get the Create Action entity from the ROCrate
    """
    for entity in entity.get_entities():
        if entity.type == "CreateAction":
            return entity
    return None

def get_results_dict(entity:ROCrate):
    """
    Get the results dictionary from the Create Action entity
    """
    createAction = get_Create_Action(entity)
    results= {}
    if "result" in createAction: # It is not necessary to have inputs/objects in Create Action
        temp = createAction["result"]
    else:
        return None

    for result in temp:
        results[result["name"]] = result.id
    return results

def check_slurm_cluster() -> tuple[bool, str]:
    """
    To check if the program is running on a SLURM cluster.

    Returns:
        tuple[bool, str]: tuple of a boolean indicating if the program
            is running on a SLURM cluster and a message.
    """
    try:
        result = subprocess.run(['squeue'], capture_output=True, text=True)
        if result.returncode == 0:
            return True, result.stdout
    except Exception as e:
        return False, str(e)
