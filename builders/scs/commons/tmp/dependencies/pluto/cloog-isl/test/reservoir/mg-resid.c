/* Generated from ./reservoir/mg-resid.cloog by CLooG 0.18.1-2-g43fc508 gmp bits in 0.02s. */
if ((M >= 1) && (N >= 3) && (O >= 3)) {
    if (M >= 3) {
        for (c2=2; c2<=O-1; c2++) {
            for (c6=1; c6<=M; c6++) {
                S1(c2,2,c6);
                S2(c2,2,c6);
            }
            for (c4=4; c4<=2*N-3; c4++) {
                for (c6=1; c6<=M; c6++) {
                    if ((c4+1)%2 == 0) {
                        S1(c2,((c4+1)/2),c6);
                        S2(c2,((c4+1)/2),c6);
                    }
                }
                for (c6=2; c6<=M-1; c6++) {
                    if (c4%2 == 0) {
                        S3(c2,(c4/2),c6);
                    }
                }
            }
            for (c6=2; c6<=M-1; c6++) {
                S3(c2,(N-1),c6);
            }
        }
    }
    if (M <= 2) {
        for (c2=2; c2<=O-1; c2++) {
            for (c4=3; c4<=2*N-3; c4++) {
                for (c6=1; c6<=M; c6++) {
                    if ((c4+1)%2 == 0) {
                        S1(c2,((c4+1)/2),c6);
                        S2(c2,((c4+1)/2),c6);
                    }
                }
            }
        }
    }
}
