package matmul.input.files;


@SuppressWarnings("serial")
public class MatmulAppException extends Exception {
	
	public MatmulAppException() {
		super("unknown");
	}
	
	public MatmulAppException( String _s ) {
		super(_s);
	}
	
}
