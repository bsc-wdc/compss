




#ifndef _BSD_BASE64_H
#define _BSD_BASE64_H

#ifdef __cplusplus
extern "C" {
#endif

int gs_b64_ntop(char const *src, size_t srclength, char *target, size_t targsize);
int gs_b64_pton(char const *src, char *target, size_t targsize);

#ifdef __cplusplus
}
#endif

#endif /* _BSD_BASE64_H */
