int i, j, **A, *B, *C, *D;

for (i = 0; i < 42; ++i)
    for (j = 0; j < 2*i; ++j)
        A[i][j] = B[i] + C[j] * D[i + j];
for (int i = 0; i < 42; ++i)
    D[i * j] += A[i][j];

