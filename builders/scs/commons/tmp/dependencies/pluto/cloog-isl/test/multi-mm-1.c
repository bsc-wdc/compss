/* Generated from /home/skimo/git/cloog/test/multi-mm-1.cloog by CLooG 0.14.0-284-ga90f184 gmp bits in 0.00s. */
for (i=0; i<=M; i++) {
    for (j=0; j<=min(N,i); j++) {
        S1(i,j);
        S2(i,j);
    }
    for (j=N+1; j<=i; j++) {
        S1(i,j);
    }
}
