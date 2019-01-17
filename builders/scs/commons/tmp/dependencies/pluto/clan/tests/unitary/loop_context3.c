#pragma scop
for (i = max(2, max(n, ceild(n, 2))); i <= n; i++)
    a = 0;
#pragma endscop
