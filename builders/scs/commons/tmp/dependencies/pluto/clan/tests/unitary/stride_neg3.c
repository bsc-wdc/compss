#pragma scop
for (i = 0; i <= N; i++)
    for (j = min(N - 42, min(M, P)); j >= Q; j -= 2)
        a[i][j] = i;
#pragma endscop
