/* Generated from ./reservoir/cholesky2.cloog by CLooG 0.18.1-2-g43fc508 gmp bits in 0.01s. */
if (M >= 1) {
    if (M >= 2) {
        S1(1);
    }
    for (c2=2; c2<=min(3,M); c2++) {
        S2(1,c2);
    }
    if (M == 1) {
        S1(1);
    }
    for (c2=4; c2<=3*M-4; c2++) {
        if ((c2+1)%3 == 0) {
            S1(((c2+1)/3));
        }
        for (c4=ceild(c2+2,3); c4<=min(M,c2-2); c4++) {
            for (c6=ceild(c2-c4+2,2); c6<=min(c4,c2-c4); c6++) {
                S3((c2-c4-c6+1),c4,c6);
            }
        }
        for (c4=ceild(c2+4,3); c4<=min(M,c2); c4++) {
            if ((c2+c4)%2 == 0) {
                S2(((c2-c4+2)/2),c4);
            }
        }
    }
    for (c2=max(2*M,3*M-3); c2<=3*M-2; c2++) {
        S3((c2-2*M+1),M,M);
    }
    if (M >= 2) {
        S1(M);
    }
}
