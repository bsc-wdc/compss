# Single loops
def simple1(a, b, c):
    for i in range(0, 10, 1):
        c[i] = c[i] + a[i] * b[i]


def simple2(a, b, c):
    for i in range(0, N, 1):
        c[i] = c[i] + a[N - i] * b[i + 1]


def simple3(a, b, c):
    for i in range(N, M, 1):
        c[i] = c[i] + a[i] * b[i]


def simple4(a, b, c):
    for i in range(2 * N - M + 1, 3 * N - M + 2, 1):
        c[i] = c[i] + a[i] * b[i]


def simple5(a, b, c):
    for i in range(N):
        c[i] = c[i] + a[i] * b[i]


# Different loop nests
def loop_nests1(a, b, c):
    for i in range(0, N, 1):
        for j in range(1, M, 1):
            for k in range(1, K, 1):
                c[i][j] = c[i][j] + a[i][k] * b[k][j]


def loop_nests2(a, b, c):
    for i in range(0, N, 1):
        for j in range(i, M, 1):
                c[i][j] = c[i][j] + a[i][j] * b[i][j]


# Multiple statements
def multi_statements(a, b, c):
    for i1 in range(0, 2 * N - M + K, 1):
        for j1 in range(0, M, 1):
            c[i1][j1] = c[i1][j1] + a[i1][j1] * b[i1][M - j1]
            c[i1][j1] = c[i1][j1] + b[i1][j1] * a[i1][M - j1]
        for j2 in range(0, M, 1):
            for k1 in range(0, K, 1):
                c[i1][j2] = c[i1][j2] + a[i1][k1] * b[k1][j2]
            c[i1][j2] = c[i1][j2] + a[i1][j2] * b[i1][j2]


# Function callee
def func_call1(a, b, c):
    for i in range(0, 10, 1):
        void_func()


def func_call2(a, b, c):
    for i in range(0, 10, 1):
        multiply(c[i], a[i], b[i])


def func_call3(a, b, c):
    for i in range(0, 10, 1):
        c[i] = void_func()


def func_call4(a, b, c):
    for i in range(0, 10, 1):
        c[i] = multiply(c[i], a[i], b[i])


def func_call5(a, b, c):
    for i in range(0, 10, 1):
        c[i], c[i + 1] = multiply(c[i], a[i], b[i])


def void_func():
    return "Hello World"


def multiply(c, a, b):
    return c + a * b
