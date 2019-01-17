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

#ifndef _Matop_h_
#define _Matop_h_

#if defined(__cplusplus)
extern "C" {
#endif

/* computes c = lcm(a,b) using Gcd(a,b,&c) */
extern void Lcm3(Value a, Value b, Value *c);
extern Matrix *AddANullColumn ( Matrix *M );
extern Matrix *AddANullRow ( Matrix *M );
extern void ExchangeColumns ( Matrix *M, int Column1, int Column2 );
extern void ExchangeRows ( Matrix *M, int Row1, int Row2 );
extern int findHermiteBasis ( Matrix *M, Matrix **Result );
extern Matrix *Identity ( unsigned size );
extern Bool isinHnf ( Matrix *A );
extern Bool isIntegral ( Matrix *A );
extern Value *Lcm (Value i, Value j);
extern Matrix *Matrix_Copy(Matrix const *Src);
extern void PutColumnFirst ( Matrix *X, int Columnnumber );
extern void PutColumnLast ( Matrix *X, int Columnnumber );
extern void PutRowFirst ( Matrix *X, int Rownumber );
extern void PutRowLast ( Matrix *X, int Rownumber );
extern Matrix *RemoveNColumns ( Matrix *M, int FirstColumnnumber, int NumColumns );
extern Matrix *RemoveColumn ( Matrix *M, int Columnnumber );
extern Matrix *RemoveRow ( Matrix *M, int Rownumber );
extern Matrix *Transpose ( Matrix *A );

#if defined(__cplusplus)
}
#endif

#endif /* _Matop_h_ */
