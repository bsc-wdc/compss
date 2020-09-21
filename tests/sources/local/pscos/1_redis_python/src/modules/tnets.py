from pycompss.api.task import task
from pycompss.api.constraint import constraint
from storage.api import StorageObject
from pycompss.api.parameter import *

class TNet(StorageObject):

    def __init__(self):
        super(TNet, self).__init__()

    def main_extract_features(self, bs, image_paths, pooled, mean):
        return {'a':[1, 2, 3, 4], 'b':[5, 6, 7, 8]}

class EA(TNet):

    def __init__(self):
        super(EA, self).__init__()

    @constraint(computing_units="1")
    @task(target_direction=IN, returns=object)
    def extract_features(self, bs, image_paths, pooled, mean):
        return self.main_extract_features(bs, image_paths, pooled, mean)
