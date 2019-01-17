#pragma scop
for (int i = 0; i < N; i++) {
    if (i < 5 || i > 10) {
        S[i] = S[i - 1];
    }
    S[i] = S[i + 1];
}
#pragma endscop
