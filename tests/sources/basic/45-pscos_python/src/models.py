from pycompss.storage.Object import SCO

class mySO(SCO):
    value = 0
    def __init__(self, v):
        self.value = v

    def get(self):
        return self.value

    def put(self, v):
        self.value = v
