/* Generated from ./reservoir/pingali5.cloog by CLooG 0.18.1-2-g43fc508 gmp bits in 0.01s. */
if (M >= 2) {
    for (c2=3; c2<=2*M-3; c2++) {
        for (c4=ceild(c2+3,2); c4<=M; c4++) {
            for (i=ceild(c2+1,2); i<=min(c2-1,c4-1); i++) {
                S1(i,(c2-i),c4);
            }
        }
        for (c4=max(1,c2-M); c4<=floord(c2-1,2); c4++) {
            S2((c2-c4),c4);
        }
        for (c4=ceild(c2+3,2); c4<=M; c4++) {
            for (i=ceild(c2+1,2); i<=min(c2-1,c4-1); i++) {
                S3(i,(c2-i),c4);
            }
        }
    }
    for (c2=max(M+1,2*M-2); c2<=2*M-1; c2++) {
        S2(M,(c2-M));
    }
}
