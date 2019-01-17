#pragma scop
/* Clay
reorder([], [1,0]);
*/
for (i = 0 ; i <= N ; i++) {
    t = 0;
    a[i] = t;
}
for (i = 0 ; i <= N ; i++) {
    t = 0;
    b[i] = t;
}
#pragma endscop
