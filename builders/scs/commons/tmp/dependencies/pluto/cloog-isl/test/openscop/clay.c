/* Useful macros. */
#define floord(n,d) (((n)<0) ? -((-(n)+(d)-1)/(d)) : (n)/(d))
#define ceild(n,d)  (((n)<0) ? -((-(n))/(d)) : ((n)+(d)-1)/(d))
#define max(x,y)    ((x) > (y) ? (x) : (y))
#define min(x,y)    ((x) < (y) ? (x) : (y))

#ifdef TIME
#define IF_TIME(foo) foo;
#else
#define IF_TIME(foo)
#endif

/* Scattering iterators. */
int __ii0;

for (__ii0=0; __ii0<=10; __ii0++) {
    for (i=4*__ii0; i<=min(41,4*__ii0+3); i++) {
        S(i);
    }
}
