package matmul.exceptions;

public class MatmulException extends Exception {

    private static final long serialVersionUID = 2L;


    public MatmulException() {
        super();
    }

    public MatmulException(Exception e) {
        super(e);
    }

    public MatmulException(String msg) {
        super(msg);
    }

    public MatmulException(String msg, Exception e) {
        super(msg, e);
    }
    
}
