#pragma scop
for (i = 1; i <= N; i += 42)
    a[i] = 0;
#pragma endscop
