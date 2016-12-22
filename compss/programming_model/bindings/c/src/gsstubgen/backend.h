#ifndef BACKEND_H
#define BACKEND_H

void generate_prolog(void);
void generate_epilogue(void);
void generate_body(void);

void generate_c_constraints_prolog(void);
void generate_c_constraints_body(void);
void generate_c_constraints_epilogue(void);

#endif /* BACKEND_H */
