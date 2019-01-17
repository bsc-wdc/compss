// candl options | -scalexp 1

#pragma scop
for (i = 0 ; i <= N ; i++) {
    t = 0;
    a[i] = t;
}
for (i = 0 ; i <= N ; i++) {
    t = 0;
    b[i] = t;
}
#pragma endscop
