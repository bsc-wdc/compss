import pycompss.interactive as ipycompss
from pycompss.api.task import task
from pycompss.api.parameter import *
from pycompss.api.api import *

class myClass(object):
   def __init__(self):
      self.model = None
       
   @task(net=IN, returns=int, is_replicated=True)
   def build(self, net):
       return 1

   @task(target_direction=IN, dataset=IN, returns=int)
   def fit(self, dataset):
       print("Hola")
       return 1

