#pragma scop
for (i1 = 0; i1 < n1; ++i1)
    a1 = 0;
#pragma endscop

something;

#pragma scop
for (i2 = 0; i2 < n2; ++i2)
    a2 = 0;
#pragma endscop

