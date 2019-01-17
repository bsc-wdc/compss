/* Generated from ./gesced2.cloog by CLooG 0.18.1-2-g43fc508 gmp bits in 0.01s. */
for (c1=1; c1<=4; c1++) {
    for (c2=5; c2<=M-10; c2++) {
        S1(c1,c2);
    }
}
for (c1=5; c1<=M-10; c1++) {
    for (c2=-c1+1; c2<=4; c2++) {
        S2((c1+c2),c1);
    }
    for (c2=5; c2<=min(M-10,-c1+M); c2++) {
        S1(c1,c2);
        S2((c1+c2),c1);
    }
    for (c2=-c1+M+1; c2<=M-10; c2++) {
        S1(c1,c2);
    }
    for (c2=M-9; c2<=-c1+M; c2++) {
        S2((c1+c2),c1);
    }
}
for (c1=M-9; c1<=M; c1++) {
    for (c2=5; c2<=M-10; c2++) {
        S1(c1,c2);
    }
}
