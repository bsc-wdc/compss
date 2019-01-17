#pragma scop
for (i = 0; i <= N; i++)
    for (j = max(N - 42, max(M, P)); j <= N; j += 2)
        a[i][j] = i;
#pragma endscop
