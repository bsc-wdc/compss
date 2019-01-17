#pragma scop
for (i = 0; i <= N; i += 1)
    for (j = i; j <= N; j += 2)
        for (k = j; k <= N; k += 3)
            a[i][j][k] = 0;
#pragma endscop
