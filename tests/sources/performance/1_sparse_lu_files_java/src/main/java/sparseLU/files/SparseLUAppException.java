package sparseLU.files;

@SuppressWarnings("serial")
public class SparseLUAppException extends Exception {

    public SparseLUAppException() {
        super("unknown");
    }

    public SparseLUAppException(String _s) {
        super(_s);
    }

}
