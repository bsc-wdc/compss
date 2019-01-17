#pragma scop
/* Clay
   reorder([0],[1,0]);
 */
for (i = 0; i < N; i++) {
    if (i < 5 || i > 10)
        S[i] = S[i - 1];
    if (i > 5 && i < 10)
        S[i - 1] = S[i - 2];
}
#pragma endscop
