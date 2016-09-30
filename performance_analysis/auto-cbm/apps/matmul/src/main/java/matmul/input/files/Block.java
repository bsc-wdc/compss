package matmul.input.files;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.StringTokenizer;


public class Block {	
	private final int BLOCK_SIZE = 2;
	private int bCols, bRows;
	private double [][] data;

	public Block() {
		bRows = bCols = BLOCK_SIZE;
		data = new double[bRows][bCols];

		for( int i=0;i<bRows;i++) {
			for( int j=0;j<bCols;j++) {
				data[i][j] = 0.0;
			}
		}
	}

	public Block( int _bRows, int _bCols ) {
		bRows = _bRows;
		bCols = _bCols;
		data = new double[bRows][bCols];

		for( int i=0;i<bRows;i++) {
			for( int j=0;j<bCols;j++) {
				data[i][j] = 0.0;
			}
		}
	}

	public Block( String filename ) {
		bRows = bCols = BLOCK_SIZE;
		data = new double[bRows][bCols];
		try	{
			FileReader filereader = new FileReader( filename );
			BufferedReader br = new BufferedReader( filereader );			// Get a buffered reader. More Efficient
			StringTokenizer tokens;
			String nextLine;

			for(int i = 0; i < bRows; i++) {
				nextLine = br.readLine();
				tokens = new StringTokenizer( nextLine );
				for( int j = 0; j < bCols && tokens.hasMoreTokens(); j++) {
					data[i][j] = Double.parseDouble( tokens.nextToken() );
				}
			}
			br.close();
			filereader.close();
		}
		catch (FileNotFoundException fnfe) {
			fnfe.printStackTrace();
			System.exit(-1);
		}
		catch (IOException ioe) {
			ioe.printStackTrace();
			System.exit(-1);
		}

	}

	protected void printBlock() {
		for( int i=0;i<bRows;i++) {
			for( int j=0;j<bCols;j++) {
				System.out.print( data[i][j] + " " );
			}
			System.out.println();
		}
	}

	public void blockToDisk( String filename ) {
		//System.out.println( "bRows : " + bRows );
		//System.out.println( "bCols : " + bCols );
		try	{
			FileOutputStream fos = new FileOutputStream( filename );
			
			for ( int i = 0; i < bRows; i++ ) {
				for ( int j = 0; j < bCols; j++ ) {
					String str = ( new Double( data[i][j] ) ).toString() + " ";
					fos.write( str.getBytes() );
				}
				fos.write( "\n".getBytes() );
			}
			fos.close();
		}
		catch ( FileNotFoundException fnfe ) {
			fnfe.printStackTrace();
			System.exit(-1);
		}
		catch ( IOException ioe ) {
			ioe.printStackTrace();
			System.exit(-1);
		}
	}

	public void sub ( Block b ) {
		for( int i = 0; i < bRows; i++ ) {
			for( int j = 0; j < bCols; j++ ) {
				this.data[i][j] -= b.data[i][j];
			}
		}
	}

	public void multiplyAccum ( Block a, Block b ) {
		for( int i = 0; i < this.bRows; i++ )			// rows
			for( int j = 0; j < this.bCols; j++ )		// cols
				for ( int k = 0; k < this.bCols; k++ )	// cols
					this.data[i][j] += a.data[i][k] * b.data[k][j];
	}

	public void cholesky() throws MatmulAppException {
		double tmp;
		Block L = new Block();
		
		for( int j = 0; j < BLOCK_SIZE; j++) {
			tmp = 0;
			for( int i = 0; i < j; i++)
				tmp += L.data[j][i] * L.data[j][i];
			L.data[j][j] = this.data[j][j] - tmp;

			for( int i = j + 1; i < BLOCK_SIZE; i++) {
				tmp = 0;
				for( int k = 0; k < j; k++)
					tmp += L.data[i][k] * L.data[j][k];
				L.data[i][j] = this.data[i][j] - tmp;
			}
			if(L.data[j][j] <= 0) {
				throw new MatmulAppException( "Diagonal element L[" + j + "][" + j + "] is negative" );
			}
			L.data[j][j] = Math.sqrt(L.data[j][j]);

			for( int i = j + 1; i < BLOCK_SIZE; i++)
				L.data[i][j] = L.data[i][j] / L.data[j][j];
		}
		this.data = L.data;
	}
	
	public void inverse() throws MatmulAppException {
		Block R = new Block();
		Block I = new Block( BLOCK_SIZE, 2 * BLOCK_SIZE );
		
		boolean found = false;
		double swap;

		for(int l = 0; l < this.bRows; l++) {
			for(int j = 0; j < this.bCols; j++) {
				I.data[l][j] = this.data[l][j];
			}
			I.data[l][l + this.bCols] = 1;
		}

		for(int j = 0; j < this.bCols; j++) {
			int i = j;
			found = false;
			while(!found && i < I.bRows ) {
				if(I.data[i][j] != 0)
				found = true;
				else i++;
			}
			if(!found) {
				throw new MatmulAppException( "We can't find the inverse of this matrix..." );
			}
			swap = I.data[i][j];
			for(int aux = 0; aux < I.bCols; aux++) {
				I.data[i][aux] /= swap;
			}

			for(int k = 0; k < this.bRows; k++) {
				if(k != i) {
					swap = I.data[k][j];
					for(int aux = 0; aux < I.bCols; aux++)
					I.data[k][aux] -= swap * I.data[i][aux];
				}
			}
			if(i != j)  {
				for(int aux = 0; aux < I.bCols; aux++) {
					swap = I.data[i][aux];
					I.data[i][aux] = I.data[j][aux];
					I.data[j][aux] = swap;
				}
			}
		}

		for(int l = 0; l < R.bRows; l++) {
			for(int j = 0; j < R.bCols; j++) {
				R.data[l][j] = I.data[l][j + this.bCols];
			}
		}

		this.data = R.data;
	}
	
	public void reverseColumns() {
		Block tmp = new Block();
		for(int i = 0; i < this.bRows; i++) {
			for(int j = 0; j < this.bCols; j++) {
				tmp.data[i][j] = this.data[i][(BLOCK_SIZE-1)-j];
			}
		}
		this.data = tmp.data;
	}
}
