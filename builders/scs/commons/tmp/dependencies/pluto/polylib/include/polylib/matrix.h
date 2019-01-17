/*
    This file is part of PolyLib.

    PolyLib is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    PolyLib is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with PolyLib.  If not, see <http://www.gnu.org/licenses/>.
*/

#ifndef _matrix_H_
#define _matrix_H_

#if defined(__cplusplus)
extern "C" {
#endif

extern Matrix *Matrix_Alloc(unsigned NbRows, unsigned NbColumns);
extern void Matrix_Free(Matrix *Mat);
extern void Matrix_Extend(Matrix *Mat, unsigned NbRows);
extern void Matrix_Print(FILE * Dst, const char *Format, Matrix *Mat);
extern Matrix *Matrix_Read_Input(Matrix *Mat);
extern Matrix *Matrix_Read(void);
extern void right_hermite(Matrix *A,Matrix **Hp,Matrix **Up,Matrix
			  **Qp);
extern void left_hermite(Matrix *A,Matrix **Hp,Matrix **Qp,Matrix
			 **Up);
extern int MatInverse(Matrix *M,Matrix *MInv);
extern void rat_prodmat(Matrix *S,Matrix *X,Matrix *P);
extern void Matrix_Vector_Product(Matrix *mat,Value *p1,Value *p2);
extern void Vector_Matrix_Product(Value *p1,Matrix *mat,Value *p2);
extern void Matrix_Product(Matrix *mat1,Matrix *mat2,Matrix *mat3);
extern int Matrix_Inverse(Matrix *Mat,Matrix *MatInv);

#if defined(__cplusplus)
}
#endif

#endif /* _matrix_H_ */
