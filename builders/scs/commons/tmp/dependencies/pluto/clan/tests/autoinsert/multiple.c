int i, j, k, **A;

for (i = 0; i < 42; ++i)
    for (j = i; j < 42; ++j)
        A[i][j] = i * j;

for (k = 0; k < 10; ++k)
    for (j = 0; j < 42; ++j)
        A[i * j] = 42;

for (i = 0; i < 10; ++i)
    for (j = 0; j < 42; ++j)
        A[i][j] = i * j;
