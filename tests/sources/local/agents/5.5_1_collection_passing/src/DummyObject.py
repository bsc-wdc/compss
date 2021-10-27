class DummyObject:
    def __init__(self, n=0):
        self.n = n
    
    def __str__(self):
        return "D["+str(self.n)+"]"
    
    def __repr__(self):
        return str(self)