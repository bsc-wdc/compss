#pragma scop
for (int i = 0; i < 42; i+=2) {
    if (i%3 == 0)
        S1(i);
    if (i%5 == 0)
        S2(i);
}
#pragma endscop
