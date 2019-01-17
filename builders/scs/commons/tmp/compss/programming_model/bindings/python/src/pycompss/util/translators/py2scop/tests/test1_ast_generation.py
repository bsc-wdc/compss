# Without loops
def empty(a, b, c):
    print("HEADER")
    print("FOOTER")


# Single loop with different header / footer options
def simple1(a, b, c):
    print("HEADER")
    for i in range(1, 10, 1):
        c[i] = c[i] + a[i] * b[i]
    print("FOOTER")


def simple2(a, b, c):
    for i in range(1, 10, 1):
        c[i] = c[i] + a[i] * b[i]
    print("FOOTER")


def simple3(a, b, c):
    print("HEADER")
    for i in range(1, 10, 1):
        c[i] = c[i] + a[i] * b[i]


def simple4(a, b, c):
    for i in range(1, 10, 1):
        c[i] = c[i] + a[i] * b[i]


# 2 loops with different intermediate options
def intermediate1(a, b, c):
    print("HEADER")
    for i1 in range(1, 10, 1):
        c[i1] = c[i1] + a[i1] * b[i1]
    for i2 in range(1, 10, 1):
        c[i2] = c[i2] + a[i2] * b[i2]
    print("FOOTER")


def intermediate2(a, b, c):
    print("HEADER")
    for i1 in range(1, 10, 1):
        c[i1] = c[i1] + a[i1] * b[i1]
    # A comment
    for i2 in range(1, 10, 1):
        c[i2] = c[i2] + a[i2] * b[i2]
    print("FOOTER")


def intermediate3(a, b, c):
    print("HEADER")
    for i1 in range(1, 10, 1):
        c[i1] = c[i1] + a[i1] * b[i1]
    print("INTER")
    for i2 in range(1, 10, 1):
        c[i2] = c[i2] + a[i2] * b[i2]
    print("FOOTER")


# Different loop nests
def loop_nests1(a, b, c):
    print("HEADER")
    for i1 in range(1, 10, 1):
        for j1 in range(1, 20, 1):
            c[i1][j1] = c[i1][j1] + a[i1][j1] * b[i1][j1]
    print("FOOTER")


def loop_nests2(a, b, c):
    print("HEADER")
    for i1 in range(1, 10, 1):
        for j1 in range(1, 20, 1):
            c[i1][j1] = c[i1][j1] + a[i1][j1] * b[i1][j1]
        for j2 in range(1, 30, 1):
            for k1 in range(1, 40, 1):
                c[i1][j2] = c[i1][j2] + a[i1][k1] * b[k1][j2]
    print("FOOTER")


# Complex test
def complex_loops(a, b, c):
    print("HELLO")
    for i1 in range(1, 10, 1):
        for j1 in range(1, 20, 1):
            c[i1][j1] = c[i1][j1] + a[i1][j1] * b[i1][j1]
        for j2 in range(1, 30, 1):
            for k1 in range(1, 40, 1):
                c[i1][j2] = c[i1][j2] + a[i1][k1] * b[k1][j2]
    for i2 in range(1, 50, 1):
        c[i2] = c[i2] + a[i2] * b[i2]
    print("INTER")
    for i3 in range(1, 60, 1):
        c[i3] = c[i3] + a[i3] * b[i3]
    print("INTER2")
    if a != b:
        for i4 in range(1, 70, 1):
            c[i4] = c[i4] + a[i4] * b[i4]
    print("BYE")
