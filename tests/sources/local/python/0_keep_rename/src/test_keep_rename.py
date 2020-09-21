# For better print formatting
from __future__ import print_function

# Imports
from pycompss.api.task import task
from pycompss.api.parameter import *

FILE_NAME="dummy_file.txt"

CONTENT="Content"

RENAMED_SUFFIX=".IT"

@task(filename={Type:FILE_OUT, Keep_rename:True})
def write_file_kr(filename, text):
    print("File path is :" + filename)
    if filename.endswith(RENAMED_SUFFIX):
        f = open(filename, "w")
        f.write(text)
        f.close()
    else :
        raise Exception("Incorrect filename. Not renamed file name")

@task(filename=FILE_IN)
def read_file(filename):
    print("File path is :" + filename)
    if not filename.endswith(FILE_NAME):
        raise Exception("Incorrect filename. Renamed file name")
    else :
        f = open(filename, "r")
        content = f.read()
        f.close()
        if (content != CONTENT):
            raise Exception(" Keep rename file is not working because content is not the expected(" + content + ")")


@task(structure={Type:COLLECTION_FILE_OUT, Keep_rename:True})
def write_files_kr(structure, text):
    for name in structure:
        if not name.endswith(RENAMED_SUFFIX):
            raise Exception("Incorrect filename. Not renamed file name")
        print("File path is :" + name)
        f = open(name, "w")
        f.write(text)
        f.close()

@task(structure=COLLECTION_FILE_IN)
def read_files(structure):
    for name in structure:
        if name.endswith(RENAMED_SUFFIX):
            raise Exception("Incorrect filename. Renamed file name")
        else:
            f = open(name, "r")
            content = f.read()
            f.close()
            if (content != CONTENT):
                raise Exception(" Keep rename file is not working because content is not the expected(" + content + ")")


def main():
    filename=FILE_NAME
    write_file_kr(filename, CONTENT)
    read_file(filename)

    files_out = ["file1/file_out.txt", "file2/file_out.txt"]
    write_files_kr(files_out, CONTENT)
    read_files(files_out)

if __name__ == "__main__":
    main()
