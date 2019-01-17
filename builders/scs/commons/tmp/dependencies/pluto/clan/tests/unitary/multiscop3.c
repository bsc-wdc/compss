#pragma scop
for (i1 = 0; i1 < n1; ++i1)
    a1 = 0;
#pragma endscop

something;

#pragma scop
a2 = 0;
for (i2 = 0; i2 < n2; ++i2)
    a2 = 0;
#pragma endscop

something;
something;

#pragma scop
a3 = 0;
for (i3 = 0; i3 < n3; ++i3)
    a3 = 0;
a3 = 0;
#pragma endscop

