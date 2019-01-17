#pragma scop
for (i = N; i >= 0; i --)
    for (j = N; j >= i; j -= 2)
        for (k = N; k >= j; k -= 3)
            a[i][j][k] = 0;
#pragma endscop
