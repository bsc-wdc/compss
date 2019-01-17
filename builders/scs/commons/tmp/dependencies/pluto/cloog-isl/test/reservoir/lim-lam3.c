/* Generated from ./reservoir/lim-lam3.cloog by CLooG 0.18.1-2-g43fc508 gmp bits in 0.01s. */
S4(1);
for (c2=9; c2<=min(13,5*M-1); c2++) {
    if (c2 <= M+7) {
        S2((c2-7),1);
    }
    if (c2 == 10) {
        S4(2);
    }
    if (c2 <= 3*M+3) {
        if (c2%3 == 0) {
            S3(((c2-3)/3),1);
        }
    }
}
for (c2=14; c2<=5*M-1; c2++) {
    for (c4=max(2,ceild(c2-M-3,4)); c4<=min(floord(c2-8,3),M-1); c4++) {
        for (c6=max(1,ceild(c2-2*c4-M-5,2)); c6<=min(floord(c2-3*c4-6,2),c4-1); c6++) {
            S1((c2-2*c4-2*c6-5),c4,c6);
        }
    }
    for (c4=max(1,ceild(c2-M-3,4)); c4<=floord(c2-4,5); c4++) {
        S2((c2-4*c4-3),c4);
    }
    if (c2%5 == 0) {
        S4((c2/5));
    }
    for (c4=max(1,ceild(c2-3*M-1,2)); c4<=floord(c2-4,5); c4++) {
        if ((c2+c4+2)%3 == 0) {
            S3(((c2-2*c4-1)/3),c4);
        }
    }
}
if (M >= 2) {
    S4(M);
}
