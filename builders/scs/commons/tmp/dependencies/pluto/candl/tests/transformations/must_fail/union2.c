#pragma scop
/* Clay
   iss([0], {1||-5});
   split([0,0],1);
   reorder([], [1,0]);
 */
for (i = 0; i < N; i++) {
    S[i] = S[i - 1];
}
#pragma endscop
