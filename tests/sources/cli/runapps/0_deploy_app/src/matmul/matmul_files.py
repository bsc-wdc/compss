#!/usr/bin/python
#
#  Copyright 2002-2019 Barcelona Supercomputing Center (www.bsc.es)
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

# -*- coding: utf-8 -*-


def initialize_variables():
    for i in range(MSIZE):
        A.append([])
        B.append([])
        C.append([])
        for j in range(MSIZE):
            A[i].append("A.%d.%d" % (i, j))
            B[i].append("B.%d.%d" % (i, j))
            C[i].append("C.%d.%d" % (i, j))


def fill_matrices():
    for c in ['A', 'B', 'C']:
        for i in range(MSIZE):
            for j in range(MSIZE):
                tmp = "%s.%d.%d" % (c, i, j)
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


# ## MAIN PROGRAM ## #

if __name__ == "__main__":
    import sys
    from matmul_tasks import multiply

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
