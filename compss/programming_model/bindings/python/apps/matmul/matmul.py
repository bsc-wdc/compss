


'''
@author: etejedor
'''

### HELPER FUNCTIONS ###

def initialize_variables():
    for i in range(MSIZE):
        A.append([])
        B.append([])
        C.append([])
        for j in range(MSIZE):
            A[i].append("A.%d.%d" % (i,j))
            B[i].append("B.%d.%d" % (i,j))
            C[i].append("C.%d.%d" % (i,j))

def fill_matrices():
    for c in ['A', 'B', 'C']:
        for i in range(MSIZE):
            for j in range(MSIZE):
                tmp = "%s.%d.%d" % (c,i,j)
                f = open(tmp, 'w')
                for _ in range(BSIZE):
                    for jj in range(BSIZE):
                        if c == 'C':
                            f.write('0.0')
                        else:
                            f.write('2.0')
                        if jj < BSIZE - 1:
                            f.write(' ')
                    f.write('\n')
                f.close()

def load_block(fi):
    b = []
    f = open(fi, 'r')
    for line in f:
        split_line = line.split(' ')
        r = []
        for num in split_line:
            r.append(float(num))
        b.append(r)
    f.close()
    return b

def store_block(b, fi, size):
    f = open(fi, 'w')
    for row in b:
        for j in range(size):
            num = row[j]
            f.write(str(num))
            if j < size - 1:
                f.write(' ')
        f.write('\n')  



### TASK SELECTION ###

from pycompss.api.task import task
from pycompss.api.parameter import *

@task(fa = FILE, fb = FILE, fc = FILE_INOUT)
def multiply(fa, fb, fc, size):
    a = load_block(fa)
    b = load_block(fb)
    c = load_block(fc)
    for i in range(size):
        for j in range(size):
            for k in range(size):
                c[i][j] += a[i][k] * b[k][j];
    store_block(c, fc, size)



### MAIN PROGRAM ###

if __name__ == "__main__":
    
    import sys
    
    args = sys.argv[1:]
    
    MSIZE = int(args[0])
    BSIZE = int(args[1])
    
    A = []
    B = []
    C = []
    
    initialize_variables()
    fill_matrices()
    
    for i in range(MSIZE):
        for j in range(MSIZE):
            for k in range(MSIZE):
                multiply(A[i][k], B[k][j], C[i][j], BSIZE)
    
