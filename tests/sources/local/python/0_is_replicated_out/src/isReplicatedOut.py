import os
import time
from myclass import *
from pycompss.api.api import compss_wait_on

# Main execution
if __name__ == '__main__':
   p1 = myClass()
   p1.build(1)
   a=[]
   for i in range(0, 8):
      a.append(p1.fit(1))
   a = compss_wait_on(a)

