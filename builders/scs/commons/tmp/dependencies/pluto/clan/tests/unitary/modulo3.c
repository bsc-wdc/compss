#pragma scop
for (i = 0; i < N; ++i)
    for (j = 0; (j < N) && (i % 11 == 1); ++j) {
        if ((i % 12 == 2) && (j % 13 == 3))
            S1();
        if (((i+j) % 14 == 4) || ((i-j) % 15 == 5))
            S2();
    }
#pragma endscop
