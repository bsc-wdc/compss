/* Generated from ./nul_basic2.cloog by CLooG 0.18.1-2-g43fc508 gmp bits in 0.00s. */
for (i=2; i<=n; i+=2) {
    if (i%4 == 0) {
        S2(i,(i/4));
    }
    S1(i,(i/2));
}
