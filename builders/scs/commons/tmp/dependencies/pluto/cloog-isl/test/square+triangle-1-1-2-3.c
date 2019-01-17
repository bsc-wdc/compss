/* Generated from /home/skimo/git/cloog/test/square+triangle-1-1-2-3.cloog by CLooG 0.14.0-284-ga90f184 gmp bits in 0.00s. */
for (j=1; j<=M; j++) {
    S1(1,j);
}
for (i=2; i<=M; i++) {
    S1(i,1);
    for (j=2; j<=i; j++) {
        S1(i,j);
        S2(i,j);
    }
    for (j=i+1; j<=M; j++) {
        S1(i,j);
    }
}
