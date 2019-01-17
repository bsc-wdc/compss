#pragma scop
/* Clay
   iss([0],{1||-4});
 */
for (int i = 0; i < N; i++) {
    S[i] = S[i - 1];
}
#pragma endscop
