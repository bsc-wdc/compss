import storage.api
from storage.storage_object import StorageObject

class PSCO(StorageObject):
    def __init__(self, content = "Content"):
        super(PSCO, self).__init__()
        self.content = content

    def get_content(self):
        return self.content

    def set_content(self, content):
        self.content = content
