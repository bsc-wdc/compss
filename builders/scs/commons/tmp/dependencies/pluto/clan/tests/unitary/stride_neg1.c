#pragma scop
for (i = N; i >=1; i -= 42)
    a[i] = 0;
#pragma endscop
