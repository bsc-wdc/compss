#pragma scop
for (i = 0; i < N; ++i)
    for (j = 0; j < N; ++j)
        if (!((i > 1) || (j == 0)))
            S1();
#pragma endscop
