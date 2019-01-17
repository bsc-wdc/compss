/* Generated from /home/skimo/git/cloog/test/walters.cloog by CLooG 0.14.0-223-gad1f0a0 gmp bits in 0.01s. */
extern void hash(int);

/* Useful macros. */
#define floord(n,d) (((n)<0) ? -((-(n)+(d)-1)/(d)) : (n)/(d))
#define ceild(n,d)  (((n)<0) ? -((-(n))/(d)) : ((n)+(d)-1)/(d))
#define max(x,y)    ((x) > (y) ? (x) : (y))
#define min(x,y)    ((x) < (y) ? (x) : (y))

#define S1(i,div36,div37,div38) { hash(1); hash(i); hash(div36); hash(div37); hash(div38); }
#define S2(i,div36,div37,div38) { hash(2); hash(i); hash(div36); hash(div37); hash(div38); }
#define S3(i,div36,div37,div38) { hash(3); hash(i); hash(div36); hash(div37); hash(div38); }
#define S4(i,div36,div37,div38) { hash(4); hash(i); hash(div36); hash(div37); hash(div38); }

void test() {
    /* Original iterators. */
    int i, div36, div37, div38;
    S2(1,0,1,0);
    S4(1,0,1,0);
    S3(2,0,1,1);
    S4(2,0,1,1);
    for (i=3; i<=10; i++) {
        if ((i+2)%3 <= 1) {
            div36 = floord(i-1,3);
            if ((i+1)%3 <= 1) {
                div37 = floord(i+1,3);
                if ((i+1)%3 == 0) {
                    S3(i,div36,div37,(i+1)/3);
                    S4(i,div36,div37,(i+1)/3);
                }
            }
            if ((i+2)%3 == 0) {
                div38 = floord(i+1,3);
                S2(i,div36,(i+2)/3,div38);
                S4(i,div36,(i+2)/3,div38);
            }
        }
        if (i%3 == 0) {
            div37 = floord(i+2,3);
            div38 = floord(i+1,3);
            S1(i,i/3,div37,div38);
            S4(i,i/3,div37,div38);
        }
    }
}
