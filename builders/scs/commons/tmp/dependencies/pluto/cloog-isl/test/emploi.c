/* Generated from ../../../git/cloog/test/emploi.cloog by CLooG 0.14.0-245-gd8c1718 gmp bits in 0.01s. */
if (n >= 1) {
    if (m >= 1) {
        for (i=1; i<=n; i++) {
            S1(i);
            for (j=1; j<=m; j++) {
                S2(i,j);
            }
        }
    }
    if (m <= 0) {
        for (i=1; i<=n; i++) {
            S1(i);
        }
    }
}
