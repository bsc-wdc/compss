// candl options | -scalpriv 1

#pragma scop
for(i = 0 ; i <= N ; i++) {
    a = 0;
    b[i] = a;
}
#pragma endscop
