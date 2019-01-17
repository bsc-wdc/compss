#pragma scop
a = m;
for (i = 0; i < m; i++)
    b[i] = 0;
#pragma endscop

