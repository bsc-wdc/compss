#pragma scop
for (int i = 0; i < min(N, M); i++) {
    min(i, 2*i);
}
#pragma endscop
