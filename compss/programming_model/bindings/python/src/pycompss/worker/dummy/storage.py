"""
@author: fconejer

Storage dummy connector
=======================
    This file contains the functions that any storage that wants to be used
    with PyCOMPSs must implement
    
    storage.api code example.
"""

def getByID(obj):
    """
    This functions retrieves an object from an external storage technology
    from the obj object.
    This dummy returns the same object as submited by the parameter obj.
    @param obj: key/object of the object to be retrieved.
    @return: the real object. 
    """
    return obj

class TaskContext( object ):
    
    def __init__(self, logger, values, **kwargs):
        self.logger = logger
        self.values = values
    
    def __enter__( self ):
        # Do something prolog
    
        # Ready to start the task
        self.logger.info("Prolog finished")
        pass
    
    def __exit__( self, type, value, traceback ):
        # Do something epilog
    
        # Finished
        self.logger.info("Epilog finished")
        pass


# Old school
# 
# def start_task(values):
#     """
#     This function is called before performing the user task.
#     Take into account that return is no required since it is ignored.
#     In this dummy, it does nothing.
#     @param values: the values that the task will receive.
#     """
#     pass
# 
# 
# def end_task(values):
#     """
#     This function is called after performing the user task.
#     Take into account that return is no required since it is ignored.
#     In this dummy, it does nothing.
#     @param values: the values that the task received.
#     """
#     pass


