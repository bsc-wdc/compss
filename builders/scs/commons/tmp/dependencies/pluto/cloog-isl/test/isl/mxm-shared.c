/* Generated from ./isl/mxm-shared.cloog by CLooG 0.18.1-2-g43fc508 gmp bits in 0.17s. */
if (g4%4 == 0) {
    if ((N >= g0+t1+1) && (N >= g1+t0+1) && (t1 <= 7)) {
        for (c0=t0; c0<=min(127,N-g1-1); c0+=16) {
            S1((g0+t1),(c0+g1));
        }
    }
}
