/* Generated from /home/skimo/git/cloog/test/logo.cloog by CLooG 0.14.0-284-ga90f184 gmp bits in 0.01s. */
for (j=0; j<=7; j++) {
    S1(1,j);
}
for (i=2; i<=6; i++) {
    for (j=0; j<=i-2; j++) {
        S2(i,j);
    }
    for (j=i-1; j<=4; j++) {
        S1(i,j);
        S2(i,j);
    }
    for (j=5; j<=7; j++) {
        S1(i,j);
    }
}
for (i=7; i<=8; i++) {
    for (j=i-1; j<=7; j++) {
        S1(i,j);
    }
}
