/* Generated from ./gesced.cloog by CLooG 0.18.1-2-g43fc508 gmp bits in 0.00s. */
for (c1=1; c1<=N; c1++) {
    S1(c1);
}
for (c1=N+1; c1<=2*N; c1++) {
    for (i=1; i<=N; i++) {
        S2(i,(c1-N));
    }
}
for (c1=2*N+1; c1<=M+N; c1++) {
    for (i=1; i<=N; i++) {
        S3(i,(c1-2*N));
        S2(i,(c1-N));
    }
}
for (c1=M+N+1; c1<=M+2*N; c1++) {
    for (i=1; i<=N; i++) {
        S3(i,(c1-2*N));
    }
}
