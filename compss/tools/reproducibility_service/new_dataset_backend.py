"""
New Dataset Backend

This module asks the user if they want to add the new_dataset and gives the
directory structure of the old dataset as reference.
"""
import os
from utils import print_colored, TextColor, get_yes_or_no

def print_directory_contents(path:str, level=0):
    """
    Print the contents of a directory with indentation.
    To show the directory structure in a tree-like format

    Args:
        path (str): path to the directory
        level (int, optional): Defaults to 0.
    """
    try:
        # List all the entries in the directory
        for entry in os.listdir(path):
            entry_path = os.path.join(path, entry)
            # Print the entry (file or directory) with indentation
            print(' ' * level * 4 + entry)
            # If the entry is a directory, recursively call the function for the sub-directory
            if os.path.isdir(entry_path):
                print_directory_contents(entry_path, level + 1)
    except PermissionError:
        # Handle the case where the program does not have permission to access the directory
        print(' ' * level * 4 + '[Permission Denied]')


def new_dataset_info_collector(crate_directory:str):
    """
    Collect information about the new dataset.
    Whether the user wants to add a new dataset or not.

    Args:
        crate_directory (str): the path to the root directory of the RO-Crate.
    """
    new_dataset_path = os.path.join(crate_directory, "new_dataset")
    os.makedirs(new_dataset_path)
    print("\nPlease copy the new dataset to the 'new_dataset' folder :\n")
    print_colored("New dataset path: " + new_dataset_path, TextColor.BLUE)
    print_colored("WARNING| MAKE SURE THE NEW DATASET FOLLOWS THE SAME DIRECTORY STRUCTURE AS THE OLD DATASET", TextColor.RED)
    print("The old directory structure for reference is as follows:\n")
    print_directory_contents(os.path.join(crate_directory, "dataset"))
    check = False
    while not check:
        check = get_yes_or_no("Have you copied the new dataset to the 'new_dataset' folder?")
