int i;
#pragma scop
/* Clay
   stripmine([0,0],1,4,1);
 */
for (i = 0; i < 42; i++) {
    S(i);
}
#pragma endscop
