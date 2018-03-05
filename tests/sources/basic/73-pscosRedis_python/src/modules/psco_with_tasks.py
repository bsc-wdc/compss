import storage.api
from storage.storage_object import StorageObject
from pycompss.api.task import task

class PSCOWithTasks(StorageObject):
    def __init__(self, content = "Content"):
        super(PSCOWithTasks, self).__init__()
        self.content = content

    @task(returns=object)
    def get_content(self):
        return self.content

    @task()
    def set_content(self, content):
        self.content = content
