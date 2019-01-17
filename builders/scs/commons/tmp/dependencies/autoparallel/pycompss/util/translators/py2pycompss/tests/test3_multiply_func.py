def matmul(m_size, n_size, k_size, b_size, debug):
    # Initialize
    a = initialize(m_size, n_size, b_size, True)
    b = initialize(n_size, k_size, b_size, True)
    c = initialize(m_size, k_size, b_size, False)
    d = initialize(m_size, k_size, b_size, False)

    # Debug
    if debug:
        print("Matrix A:")
        print(a)
        print("Matrix B:")
        print(b)
        print("Matrix C:")
        print(c)

    # Perform computation
    # c, d = a*b
    for i in range(m_size):
        for j in range(k_size):
            for k in range(n_size):
                c[i][j] = multiply(c[i + 0][j], a[i][k], b[k][j])
                multiply(c[i][j], a[i][k], b[k][j])
                c[i][j], d[i][j] = multiply(c[i][j], a[i][k], b[k][j])

    # Debug
    if debug:
        print("Matrix C:")
        print(c)

    # Result
    return c


def multiply(c, a, b):
    return c + a * b
