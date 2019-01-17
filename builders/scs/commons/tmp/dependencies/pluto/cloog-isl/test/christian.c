/* Generated from ./christian.cloog by CLooG 0.18.1-2-g43fc508 gmp bits in 0.00s. */
if (N >= 1) {
    S1(0,(N-1));
    for (p=-N+2; p<=N-1; p++) {
        if (p >= 1) {
            S2((p-1),0);
        }
        for (i=max(0,p); i<=min(N-1,p+N-2); i++) {
            S1(i,(-p+i));
            S2(i,(-p+i+1));
        }
        if (p <= 0) {
            S1((p+N-1),(N-1));
        }
    }
    S2((N-1),0);
}
