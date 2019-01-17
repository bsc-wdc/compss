/* Generated from /home/skimo/git/cloog/test/test.cloog by CLooG 0.14.0-284-ga90f184 gmp bits in 0.01s. */
for (i=1; i<=2; i++) {
    for (j=1; j<=M; j++) {
        S1(i,j);
    }
}
for (i=3; i<=N; i++) {
    for (j=1; j<=min(M,i-1); j++) {
        S1(i,j);
    }
    if (i >= M+1) {
        S2(i,i);
    }
    if (i <= M) {
        S1(i,i);
        S2(i,i);
    }
    for (j=i+1; j<=M; j++) {
        S1(i,j);
    }
}
