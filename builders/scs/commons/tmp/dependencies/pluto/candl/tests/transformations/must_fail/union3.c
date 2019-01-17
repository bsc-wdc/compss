#pragma scop
/* Clay
   iss([0], {1||-5});
   reorder([0], [3,2,1,0]);
 */
for (i = 0; i < N; i++) {
    if (i < 2 || i >= 4)
        S[i] = S[i - 1];
    if (i >= 5) {
        S[i+1] = S[i];
    }
}
#pragma endscop
