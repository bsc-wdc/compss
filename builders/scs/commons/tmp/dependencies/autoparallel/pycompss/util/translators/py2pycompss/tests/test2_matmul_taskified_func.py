def matmul(m_size, n_size, k_size, b_size, debug):
    # Initialize
    a = initialize(m_size, n_size, b_size, True)
    b = initialize(n_size, k_size, b_size, True)
    c = initialize(m_size, k_size, b_size, False)

    # Debug
    if debug:
        print("Matrix A:")
        print(a)
        print("Matrix B:")
        print(b)
        print("Matrix C:")
        print(c)

    # Perform computation
    # c = a*b
    for i in range(1, m_size + 1):
        for j in range(1, k_size + 1):
            for k in range(1, n_size + 1):
                c[i][j] += a[i - 1][k - 1] * b[k - 1][j - 1]

    # Debug
    if debug:
        print("Matrix C:")
        print(c)

    # Result
    return c
