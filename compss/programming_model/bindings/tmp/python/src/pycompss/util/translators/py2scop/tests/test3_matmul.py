def matmul(mSize, nSize, kSize, bSize, debug):
    # Initialize
    a = initialize(mSize, nSize, bSize, True)
    b = initialize(nSize, kSize, bSize, True)
    c = initialize(mSize, kSize, bSize, False)

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
    for i in range(mSize):
        for j in range(kSize):
            for k in range(nSize):
                c[i][j] += a[i][k] * b[k][j]

    # Debug
    if debug:
        print("Matrix C:")
        print(c)

    # Result
    return c
