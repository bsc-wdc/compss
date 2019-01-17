#pragma scop
/* Clay
   iss([0],{1||-4});
 */
for (int i = 0; i < N; i++) {
    if (i < 7 || i > 12)
        S[i] = S[i - 1];
}
#pragma endscop
