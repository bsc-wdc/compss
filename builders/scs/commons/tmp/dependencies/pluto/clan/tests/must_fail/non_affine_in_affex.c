#pragma scop
for (i = 0; i < n; ++i)
    a[i + i*n] = 0;
#pragma endscop
