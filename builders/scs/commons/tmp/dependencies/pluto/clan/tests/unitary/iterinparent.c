#pragma scop
for (i = 0; i < 10; ++(i))
    s = 0;
#pragma endscop
