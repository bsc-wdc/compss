// Copyright © 2014 Inria, Written by Lénaïc Bagnères, lenaic.bagneres@inria.fr

// (3-clause BSD license)
// Redistribution and use in source  and binary forms, with or without
// modification, are permitted provided that the following conditions
// are met:
//
// 1. Redistributions of source code must retain the above copyright notice,
//    this list of conditions and the following disclaimer.
// 2. Redistributions in binary form must reproduce the above copyright
//    notice, this list of conditions and the following disclaimer in the
//    documentation and/or other materials provided with the distribution.
// 3. The name of the author may not be used to endorse or promote products
//    derived from this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR
// IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
// OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
// IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT,
// INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
// NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF
// THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

#include <stdio.h>
#include <limits.h>

#include <osl/int.h>
#include <osl/macros.h>


int main(int argc, char** argv) {
    if (argc > 1) {
        printf("argv are ignored\n");
    }

    int nb_fail = 0;

#ifdef OSL_GMP_IS_HERE

    int i;
    for (i = SCHAR_MIN; i <= SCHAR_MAX; ++i) {
        osl_int_t a_sp;
        osl_int_init_set_si(OSL_PRECISION_SP, &a_sp, i);
        osl_int_t a_dp;
        osl_int_init_set_si(OSL_PRECISION_DP, &a_dp, i);
        osl_int_t a_mp;
        osl_int_init_set_si(OSL_PRECISION_MP, &a_mp, i);

        int j;
        for (j = SCHAR_MIN; j <= SCHAR_MAX; ++j) {
            int error = 0;

            osl_int_t b_sp;
            osl_int_init_set_si(OSL_PRECISION_SP, &b_sp, j);
            osl_int_t b_dp;
            osl_int_init_set_si(OSL_PRECISION_DP, &b_dp, j);
            osl_int_t b_mp;
            osl_int_init_set_si(OSL_PRECISION_MP, &b_mp, j);

            osl_int_t c_sp;
            osl_int_init(OSL_PRECISION_SP, &c_sp);
            osl_int_t c_dp;
            osl_int_init(OSL_PRECISION_DP, &c_dp);
            osl_int_t c_mp;
            osl_int_init(OSL_PRECISION_MP, &c_mp);

            int const a_sp_i = osl_int_get_si(OSL_PRECISION_SP, a_sp);
            int const a_dp_i = osl_int_get_si(OSL_PRECISION_DP, a_dp);
            int const a_mp_i = osl_int_get_si(OSL_PRECISION_MP, a_mp);

            int const b_sp_i = osl_int_get_si(OSL_PRECISION_SP, b_sp);
            int const b_dp_i = osl_int_get_si(OSL_PRECISION_DP, b_dp);
            int const b_mp_i = osl_int_get_si(OSL_PRECISION_MP, b_mp);

            // osl_int_init_set_si & osl_int_init & osl_int_get_si
            if (!error) {
                int c_sp_i = osl_int_get_si(OSL_PRECISION_SP, c_sp);
                int c_dp_i = osl_int_get_si(OSL_PRECISION_DP, c_dp);
                int c_mp_i = osl_int_get_si(OSL_PRECISION_MP, c_mp);

                if (a_sp_i != a_dp_i || a_sp_i != a_mp_i) {
                    error++;
                    printf("Error osl_int_init_set_si or osl_int_get_si\n");
                }
                if (b_sp_i != b_dp_i || b_sp_i != b_mp_i) {
                    error++;
                    printf("Error osl_int_init_set_si or osl_int_get_si\n");
                }
                if (c_sp_i != c_dp_i || c_sp_i != c_mp_i || c_sp_i != 0) {
                    error++;
                    printf("Error osl_int_init or osl_int_get_si\n");
                }
            }

            // osl_int_assign
            if (!error) {
                osl_int_assign(OSL_PRECISION_SP, &c_sp, b_sp);
                osl_int_assign(OSL_PRECISION_DP, &c_dp, b_dp);
                osl_int_assign(OSL_PRECISION_MP, &c_mp, b_mp);

                if (osl_int_ne(OSL_PRECISION_SP, c_sp, b_sp)) {
                    error++;
                    printf("Error osl_int_assign\n");
                }
                if (osl_int_ne(OSL_PRECISION_DP, c_dp, b_dp)) {
                    error++;
                    printf("Error osl_int_assign\n");
                }
                if (osl_int_ne(OSL_PRECISION_MP, c_mp, b_mp)) {
                    error++;
                    printf("Error osl_int_assign\n");
                }
            }

            // osl_int_swap

            // osl_int_increment
            if (!error) {
                osl_int_increment(OSL_PRECISION_SP, &c_sp, a_sp);
                osl_int_increment(OSL_PRECISION_DP, &c_dp, a_dp);
                osl_int_increment(OSL_PRECISION_MP, &c_mp, a_mp);

                int c_sp_i = osl_int_get_si(OSL_PRECISION_SP, c_sp);
                int c_dp_i = osl_int_get_si(OSL_PRECISION_DP, c_dp);
                int c_mp_i = osl_int_get_si(OSL_PRECISION_MP, c_mp);

                if (c_sp_i != c_dp_i || c_sp_i != c_mp_i || c_sp_i != a_sp_i + 1) {
                    error++;
                    printf("Error osl_int_increment\n");
                }
            }

            // osl_int_decrement
            if (!error) {
                osl_int_decrement(OSL_PRECISION_SP, &c_sp, a_sp);
                osl_int_decrement(OSL_PRECISION_DP, &c_dp, a_dp);
                osl_int_decrement(OSL_PRECISION_MP, &c_mp, a_mp);

                int c_sp_i = osl_int_get_si(OSL_PRECISION_SP, c_sp);
                int c_dp_i = osl_int_get_si(OSL_PRECISION_DP, c_dp);
                int c_mp_i = osl_int_get_si(OSL_PRECISION_MP, c_mp);

                if (c_sp_i != c_dp_i || c_sp_i != c_mp_i || c_sp_i != a_sp_i - 1) {
                    error++;
                    printf("Error osl_int_decrement\n");
                }
            }

            // osl_int_add
            if (!error) {
                osl_int_add(OSL_PRECISION_SP, &c_sp, a_sp, b_sp);
                osl_int_add(OSL_PRECISION_DP, &c_dp, a_dp, b_dp);
                osl_int_add(OSL_PRECISION_MP, &c_mp, a_mp, b_mp);

                int c_sp_i = osl_int_get_si(OSL_PRECISION_SP, c_sp);
                int c_dp_i = osl_int_get_si(OSL_PRECISION_DP, c_dp);
                int c_mp_i = osl_int_get_si(OSL_PRECISION_MP, c_mp);

                if (c_sp_i != c_dp_i || c_sp_i != c_mp_i || c_sp_i != a_sp_i + b_sp_i) {
                    error++;
                    printf("Error osl_int_add\n");
                }
            }

            // osl_int_add_si

            // osl_int_sub
            if (!error) {
                osl_int_sub(OSL_PRECISION_SP, &c_sp, a_sp, b_sp);
                osl_int_sub(OSL_PRECISION_DP, &c_dp, a_dp, b_dp);
                osl_int_sub(OSL_PRECISION_MP, &c_mp, a_mp, b_mp);

                int c_sp_i = osl_int_get_si(OSL_PRECISION_SP, c_sp);
                int c_dp_i = osl_int_get_si(OSL_PRECISION_DP, c_dp);
                int c_mp_i = osl_int_get_si(OSL_PRECISION_MP, c_mp);

                if (c_sp_i != c_dp_i || c_sp_i != c_mp_i || c_sp_i != a_sp_i - b_sp_i) {
                    error++;
                    printf("Error osl_int_add\n");
                }
            }

            // osl_int_mul
            if (!error) {
                osl_int_mul(OSL_PRECISION_SP, &c_sp, a_sp, b_sp);
                osl_int_mul(OSL_PRECISION_DP, &c_dp, a_dp, b_dp);
                osl_int_mul(OSL_PRECISION_MP, &c_mp, a_mp, b_mp);

                int c_sp_i = osl_int_get_si(OSL_PRECISION_SP, c_sp);
                int c_dp_i = osl_int_get_si(OSL_PRECISION_DP, c_dp);
                int c_mp_i = osl_int_get_si(OSL_PRECISION_MP, c_mp);

                if (c_sp_i != c_dp_i || c_sp_i != c_mp_i || c_sp_i != a_sp_i * b_sp_i) {
                    error++;
                    printf("Error osl_int_mul\n");
                }
            }

            // osl_int_mul_si
            if (!error) {
                osl_int_mul_si(OSL_PRECISION_SP, &c_sp, a_sp, b_sp_i);
                osl_int_mul_si(OSL_PRECISION_DP, &c_dp, a_dp, b_dp_i);
                osl_int_mul_si(OSL_PRECISION_MP, &c_mp, a_mp, b_mp_i);

                int c_sp_i = osl_int_get_si(OSL_PRECISION_SP, c_sp);
                int c_dp_i = osl_int_get_si(OSL_PRECISION_DP, c_dp);
                int c_mp_i = osl_int_get_si(OSL_PRECISION_MP, c_mp);

                if (c_sp_i != c_dp_i || c_sp_i != c_mp_i || c_sp_i != a_sp_i * b_sp_i) {
                    error++;
                    printf("Error osl_int_mul_si\n");
                }
            }

            // osl_int_div_exact
            if (!error && b_sp_i != 0 && a_sp_i % b_sp_i == 0) {
                osl_int_div_exact(OSL_PRECISION_SP, &c_sp, a_sp, b_sp);
                osl_int_div_exact(OSL_PRECISION_DP, &c_dp, a_dp, b_dp);
                osl_int_div_exact(OSL_PRECISION_MP, &c_mp, a_mp, b_mp);

                int c_sp_i = osl_int_get_si(OSL_PRECISION_SP, c_sp);
                int c_dp_i = osl_int_get_si(OSL_PRECISION_DP, c_dp);
                int c_mp_i = osl_int_get_si(OSL_PRECISION_MP, c_mp);

                if (c_sp_i != c_dp_i || c_sp_i != c_mp_i || c_sp_i != a_sp_i / b_sp_i) {
                    error++;
                    printf("Error osl_int_div_exact\n");
                }
            }

            // osl_int_floor_div_q
            if (!error && b_sp_i != 0) {
                osl_int_floor_div_q(OSL_PRECISION_SP, &c_sp, a_sp, b_sp);
                osl_int_floor_div_q(OSL_PRECISION_DP, &c_dp, a_dp, b_dp);
                osl_int_floor_div_q(OSL_PRECISION_MP, &c_mp, a_mp, b_mp);

                int c_sp_i = osl_int_get_si(OSL_PRECISION_SP, c_sp);
                int c_dp_i = osl_int_get_si(OSL_PRECISION_DP, c_dp);
                int c_mp_i = osl_int_get_si(OSL_PRECISION_MP, c_mp);

                if (c_sp_i != c_dp_i || c_sp_i != c_mp_i) {
                    error++;
                    printf("Error osl_int_floor_div_q\n");
                }
            }

            // osl_int_floor_div_r
            if (!error && b_sp_i != 0) {
                osl_int_floor_div_r(OSL_PRECISION_SP, &c_sp, a_sp, b_sp);
                osl_int_floor_div_r(OSL_PRECISION_DP, &c_dp, a_dp, b_dp);
                osl_int_floor_div_r(OSL_PRECISION_MP, &c_mp, a_mp, b_mp);

                int c_sp_i = osl_int_get_si(OSL_PRECISION_SP, c_sp);
                int c_dp_i = osl_int_get_si(OSL_PRECISION_DP, c_dp);
                int c_mp_i = osl_int_get_si(OSL_PRECISION_MP, c_mp);

                if (c_sp_i != c_dp_i || c_sp_i != c_mp_i) {
                    error++;
                    printf("Error osl_int_floor_div_r\n");
                }
            }

            // osl_int_floor_div_q_r
            if (!error && b_sp_i != 0) {
                osl_int_t q_sp;
                osl_int_init(OSL_PRECISION_SP, &q_sp);
                osl_int_t q_dp;
                osl_int_init(OSL_PRECISION_DP, &q_dp);
                osl_int_t q_mp;
                osl_int_init(OSL_PRECISION_MP, &q_mp);

                osl_int_t r_sp;
                osl_int_init(OSL_PRECISION_SP, &r_sp);
                osl_int_t r_dp;
                osl_int_init(OSL_PRECISION_DP, &r_dp);
                osl_int_t r_mp;
                osl_int_init(OSL_PRECISION_MP, &r_mp);

                osl_int_floor_div_q_r(OSL_PRECISION_SP, &q_sp, &r_sp, a_sp, b_sp);
                osl_int_floor_div_q_r(OSL_PRECISION_DP, &q_dp, &r_dp, a_dp, b_dp);
                osl_int_floor_div_q_r(OSL_PRECISION_MP, &q_mp, &r_mp, a_mp, b_mp);

                int q_sp_i = osl_int_get_si(OSL_PRECISION_SP, q_sp);
                int q_dp_i = osl_int_get_si(OSL_PRECISION_DP, q_dp);
                int q_mp_i = osl_int_get_si(OSL_PRECISION_MP, q_mp);

                int r_sp_i = osl_int_get_si(OSL_PRECISION_SP, r_sp);
                int r_dp_i = osl_int_get_si(OSL_PRECISION_DP, r_dp);
                int r_mp_i = osl_int_get_si(OSL_PRECISION_MP, r_mp);

                if (q_sp_i != q_dp_i || q_sp_i != q_mp_i) {
                    error++;
                    printf("Error osl_int_floor_div_q_r\n");
                }
                if (r_sp_i != r_dp_i || r_sp_i != r_mp_i) {
                    error++;
                    printf("Error osl_int_floor_div_q_r\n");
                }
                osl_int_clear(OSL_PRECISION_SP, &q_sp);
                osl_int_clear(OSL_PRECISION_DP, &q_dp);
                osl_int_clear(OSL_PRECISION_MP, &q_mp);

                osl_int_clear(OSL_PRECISION_SP, &r_sp);
                osl_int_clear(OSL_PRECISION_DP, &r_dp);
                osl_int_clear(OSL_PRECISION_MP, &r_mp);
            }

            // osl_int_mod
            if (!error && b_sp_i != 0) {
                osl_int_mod(OSL_PRECISION_SP, &c_sp, a_sp, b_sp);
                osl_int_mod(OSL_PRECISION_DP, &c_dp, a_dp, b_dp);
                osl_int_mod(OSL_PRECISION_MP, &c_mp, a_mp, b_mp);

                int c_sp_i = osl_int_get_si(OSL_PRECISION_SP, c_sp);
                int c_dp_i = osl_int_get_si(OSL_PRECISION_DP, c_dp);
                int c_mp_i = osl_int_get_si(OSL_PRECISION_MP, c_mp);

                if (c_sp_i != c_dp_i || c_sp_i != c_mp_i) {
                    error++;
                    printf("Error osl_int_mod\n");
                }
            }

            // osl_int_gcd
            if (!error) {
                osl_int_gcd(OSL_PRECISION_SP, &c_sp, a_sp, b_sp);
                osl_int_gcd(OSL_PRECISION_DP, &c_dp, a_dp, b_dp);
                osl_int_gcd(OSL_PRECISION_MP, &c_mp, a_mp, b_mp);

                int c_sp_i = osl_int_get_si(OSL_PRECISION_SP, c_sp);
                int c_dp_i = osl_int_get_si(OSL_PRECISION_DP, c_dp);
                int c_mp_i = osl_int_get_si(OSL_PRECISION_MP, c_mp);

                if (c_sp_i != c_dp_i || c_sp_i != c_mp_i) {
                    error++;
                    printf("Error osl_int_gcd\n");
                }
            }

            // osl_int_lcm
            if (!error) {
                osl_int_lcm(OSL_PRECISION_SP, &c_sp, a_sp, b_sp);
                osl_int_lcm(OSL_PRECISION_DP, &c_dp, a_dp, b_dp);
                osl_int_lcm(OSL_PRECISION_MP, &c_mp, a_mp, b_mp);

                int c_sp_i = osl_int_get_si(OSL_PRECISION_SP, c_sp);
                int c_dp_i = osl_int_get_si(OSL_PRECISION_DP, c_dp);
                int c_mp_i = osl_int_get_si(OSL_PRECISION_MP, c_mp);

                if (c_sp_i != c_dp_i || c_sp_i != c_mp_i) {
                    error++;
                    printf("Error osl_int_lcm\n");
                }
            }

            // osl_int_oppose
            if (!error) {
                osl_int_oppose(OSL_PRECISION_SP, &c_sp, b_sp);
                osl_int_oppose(OSL_PRECISION_DP, &c_dp, b_dp);
                osl_int_oppose(OSL_PRECISION_MP, &c_mp, b_mp);

                int c_sp_i = osl_int_get_si(OSL_PRECISION_SP, c_sp);
                int c_dp_i = osl_int_get_si(OSL_PRECISION_DP, c_dp);
                int c_mp_i = osl_int_get_si(OSL_PRECISION_MP, c_mp);

                if (c_sp_i != c_dp_i || c_sp_i != c_mp_i) {
                    error++;
                    printf("Error osl_int_oppose\n");
                }
            }

            // osl_int_abs
            if (!error) {
                osl_int_abs(OSL_PRECISION_SP, &c_sp, b_sp);
                osl_int_abs(OSL_PRECISION_DP, &c_dp, b_dp);
                osl_int_abs(OSL_PRECISION_MP, &c_mp, b_mp);

                int c_sp_i = osl_int_get_si(OSL_PRECISION_SP, c_sp);
                int c_dp_i = osl_int_get_si(OSL_PRECISION_DP, c_dp);
                int c_mp_i = osl_int_get_si(OSL_PRECISION_MP, c_mp);

                if (c_sp_i != c_dp_i || c_sp_i != c_mp_i) {
                    error++;
                    printf("Error osl_int_abs\n");
                }
            }

            // osl_int_size_in_base_2
            if (!error) {
                size_t r_sp = osl_int_size_in_base_2(OSL_PRECISION_SP, b_sp);
                size_t r_dp = osl_int_size_in_base_2(OSL_PRECISION_DP, b_dp);
                size_t r_mp = osl_int_size_in_base_2(OSL_PRECISION_MP, b_mp);

                osl_int_set_si(OSL_PRECISION_SP, &c_sp, r_sp);
                osl_int_set_si(OSL_PRECISION_DP, &c_dp, r_dp);
                osl_int_set_si(OSL_PRECISION_MP, &c_mp, r_mp);

                if (r_sp != r_dp || r_sp != r_mp) {
                    error++;
                    printf("Error osl_int_size_in_base_2\n");
                }
            }

            // osl_int_size_in_base_10
//       if (!error) {
//         size_t r_sp = osl_int_size_in_base_10(OSL_PRECISION_SP, b_sp);
//         size_t r_dp = osl_int_size_in_base_10(OSL_PRECISION_DP, b_dp);
//         size_t r_mp = osl_int_size_in_base_10(OSL_PRECISION_MP, b_mp);
//
//         osl_int_set_si(OSL_PRECISION_SP, &c_sp, r_sp);
//         osl_int_set_si(OSL_PRECISION_DP, &c_dp, r_dp);
//         osl_int_set_si(OSL_PRECISION_MP, &c_mp, r_mp);
//
//         if (r_sp != r_dp || r_sp != r_mp)
//           { error++; printf("Error osl_int_size_in_base_10\n"); }
//       }
            // osl_int_eq
            if (!error) {
                int c1, c2, c3;
                c1 = osl_int_eq(OSL_PRECISION_SP, a_sp, b_sp);
                c2 = osl_int_eq(OSL_PRECISION_DP, a_dp, b_dp);
                c3 = osl_int_eq(OSL_PRECISION_MP, a_mp, b_mp);

                if (c1 != c2 || c1 != c3 || c2 != c3 || c1 != (i == j)) {
                    error++;
                    printf("Error osl_int_eq\n");
                }
            }

            // osl_int_ne
            if (!error) {
                int c1, c2, c3;
                c1 = osl_int_ne(OSL_PRECISION_SP, a_sp, b_sp);
                c2 = osl_int_ne(OSL_PRECISION_DP, a_dp, b_dp);
                c3 = osl_int_ne(OSL_PRECISION_MP, a_mp, b_mp);

                if (c1 != c2 || c1 != c3 || c2 != c3 || c1 != (i != j)) {
                    error++;
                    printf("Error osl_int_ne\n");
                }
            }

            // osl_int_pos
            if (!error) {
                int c1, c2, c3;
                c1 = osl_int_pos(OSL_PRECISION_SP, a_sp);
                c2 = osl_int_pos(OSL_PRECISION_DP, a_dp);
                c3 = osl_int_pos(OSL_PRECISION_MP, a_mp);

                if (c1 != c2 || c1 != c3 || c2 != c3 || c1 != (i > 0)) {
                    error++;
                    printf("Error osl_int_pos\n");
                }
            }

            // osl_int_neg
            if (!error) {
                int c1, c2, c3;
                c1 = osl_int_neg(OSL_PRECISION_SP, a_sp);
                c2 = osl_int_neg(OSL_PRECISION_DP, a_dp);
                c3 = osl_int_neg(OSL_PRECISION_MP, a_mp);

                if (c1 != c2 || c1 != c3 || c2 != c3 || c1 != (i < 0)) {
                    error++;
                    printf("Error osl_int_neg\n");
                }
            }

            // osl_int_zero
            if (!error) {
                int c1, c2, c3;
                c1 = osl_int_zero(OSL_PRECISION_SP, a_sp);
                c2 = osl_int_zero(OSL_PRECISION_DP, a_dp);
                c3 = osl_int_zero(OSL_PRECISION_MP, a_mp);

                if (c1 != c2 || c1 != c3 || c2 != c3 || c1 != (i == 0)) {
                    error++;
                    printf("Error osl_int_zero\n");
                }
            }

            // osl_int_one
            if (!error) {
                int c1, c2, c3;
                c1 = osl_int_one(OSL_PRECISION_SP, a_sp);
                c2 = osl_int_one(OSL_PRECISION_DP, a_dp);
                c3 = osl_int_one(OSL_PRECISION_MP, a_mp);

                if (c1 != c2 || c1 != c3 || c2 != c3 || c1 != (i == 1)) {
                    error++;
                    printf("Error osl_int_one\n");
                }
            }

            // osl_int_mone
            if (!error) {
                int c1, c2, c3;
                c1 = osl_int_mone(OSL_PRECISION_SP, a_sp);
                c2 = osl_int_mone(OSL_PRECISION_DP, a_dp);
                c3 = osl_int_mone(OSL_PRECISION_MP, a_mp);

                if (c1 != c2 || c1 != c3 || c2 != c3 || c1 != (i == -1)) {
                    error++;
                    printf("Error osl_int_mone\n");
                }
            }

            // osl_int_divisible
            if (!error && j != 0) {
                int c1, c2, c3;
                c1 = osl_int_divisible(OSL_PRECISION_SP, a_sp, b_sp);
                c2 = osl_int_divisible(OSL_PRECISION_DP, a_dp, b_dp);
                c3 = osl_int_divisible(OSL_PRECISION_MP, a_mp, b_mp);

                if (c1 != c2 || c1 != c3 || c2 != c3 || c1 != (0 == (i % j))) {
                    error++;
                    printf("Error osl_int_divisible\n");
                }
            }

            // osl_int_lt
            if (!error) {
                int c1, c2, c3;
                c1 = osl_int_lt(OSL_PRECISION_SP, a_sp, b_sp);
                c2 = osl_int_lt(OSL_PRECISION_DP, a_dp, b_dp);
                c3 = osl_int_lt(OSL_PRECISION_MP, a_mp, b_mp);

                if (c1 != c2 || c1 != c3 || c2 != c3 || c1 != (i < j)) {
                    error++;
                    printf("Error osl_int_lt\n");
                }
            }

            // osl_int_le
            if (!error) {
                int c1, c2, c3;
                c1 = osl_int_le(OSL_PRECISION_SP, a_sp, b_sp);
                c2 = osl_int_le(OSL_PRECISION_DP, a_dp, b_dp);
                c3 = osl_int_le(OSL_PRECISION_MP, a_mp, b_mp);

                if (c1 != c2 || c1 != c3 || c2 != c3 || c1 != (i <= j)) {
                    error++;
                    printf("Error osl_int_le\n");
                }
            }

            // osl_int_gt
            if (!error) {
                int c1, c2, c3;
                c1 = osl_int_gt(OSL_PRECISION_SP, a_sp, b_sp);
                c2 = osl_int_gt(OSL_PRECISION_DP, a_dp, b_dp);
                c3 = osl_int_gt(OSL_PRECISION_MP, a_mp, b_mp);

                if (c1 != c2 || c1 != c3 || c2 != c3 || c1 != (i > j)) {
                    error++;
                    printf("Error osl_int_gt\n");
                }
            }

            // osl_int_lt
            if (!error) {
                int c1, c2, c3;
                c1 = osl_int_ge(OSL_PRECISION_SP, a_sp, b_sp);
                c2 = osl_int_ge(OSL_PRECISION_DP, a_dp, b_dp);
                c3 = osl_int_ge(OSL_PRECISION_MP, a_mp, b_mp);

                if (c1 != c2 || c1 != c3 || c2 != c3 || c1 != (i >= j)) {
                    error++;
                    printf("Error osl_int_ge\n");
                }
            }

            if (error) {
                printf("Error with:\n");
                printf("\n");

                printf("a_sp = ");
                osl_int_print(stdout, OSL_PRECISION_SP, a_sp);
                printf("\n");
                printf("a_dp = ");
                osl_int_print(stdout, OSL_PRECISION_DP, a_dp);
                printf("\n");
                printf("a_mp = ");
                osl_int_print(stdout, OSL_PRECISION_MP, a_mp);
                printf("\n");
                printf("\n");

                printf("b_sp = ");
                osl_int_print(stdout, OSL_PRECISION_SP, b_sp);
                printf("\n");
                printf("b_dp = ");
                osl_int_print(stdout, OSL_PRECISION_DP, b_dp);
                printf("\n");
                printf("b_mp = ");
                osl_int_print(stdout, OSL_PRECISION_MP, b_mp);
                printf("\n");
                printf("\n");

                printf("c_sp = ");
                osl_int_print(stdout, OSL_PRECISION_SP, c_sp);
                printf("\n");
                printf("c_dp = ");
                osl_int_print(stdout, OSL_PRECISION_DP, c_dp);
                printf("\n");
                printf("c_mp = ");
                osl_int_print(stdout, OSL_PRECISION_MP, c_mp);
                printf("\n");
                printf("\n");

                nb_fail += error;
            }
            osl_int_clear(OSL_PRECISION_SP, &b_sp);
            osl_int_clear(OSL_PRECISION_DP, &b_dp);
            osl_int_clear(OSL_PRECISION_MP, &b_mp);

            osl_int_clear(OSL_PRECISION_SP, &c_sp);
            osl_int_clear(OSL_PRECISION_DP, &c_dp);
            osl_int_clear(OSL_PRECISION_MP, &c_mp);
        }
        osl_int_clear(OSL_PRECISION_SP, &a_sp);
        osl_int_clear(OSL_PRECISION_DP, &a_dp);
        osl_int_clear(OSL_PRECISION_MP, &a_mp);
    }

    printf("%s ", argv[0]);
    printf("fails = %d\n", nb_fail);

#else

    printf("%s ", argv[0]);
    printf("works only with GMP\n");

#endif

    return nb_fail;
}
