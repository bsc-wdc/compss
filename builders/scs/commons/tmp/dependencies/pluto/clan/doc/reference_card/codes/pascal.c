#include <stdio.h>
#define N 42

int main() {
    int i, j;
    int pascal[N][N];

#pragma scop
    for (i = 0; i < N; i++) {
        for (j = 0; j <= i; j++) {
            if (i == j || j == 0)
                pascal[i][j] = 1;
            else
                pascal[i][j] = pascal[i-1][j] + pascal[i-1][j-1];
        }
    }
#pragma endscop

    for (i = 0; i < N; i++) {
        for (j = 0; j <= i; j++) {
            printf("%3d ", pascal[i][j]);
        }
        printf("\n");
    }

    return 0;
}
