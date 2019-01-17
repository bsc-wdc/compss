#pragma scop
for (i = 0; i < N; ++i)
    if (i % 2 == 1)
        S1();
#pragma endscop
