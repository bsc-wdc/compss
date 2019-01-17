#pragma scop
a[0] = 0;
for (i = 1 ; i <= 5 ; i++) {
    a[i] = 0;
    b[i-1] = a[i-1];
}
#pragma endscop
