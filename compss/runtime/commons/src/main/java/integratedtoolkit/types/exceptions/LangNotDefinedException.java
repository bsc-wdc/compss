package integratedtoolkit.types.exceptions;

public class LangNotDefinedException extends RuntimeException {

    /**
     * Exceptions Version UID are 2L in all Runtime
     */
    private static final long serialVersionUID = 2L;

    private static final String LANG_UNSUPPORTED_ERR = "Error loading constraints: Language not supported";


    public LangNotDefinedException() {
        super(LANG_UNSUPPORTED_ERR);
    }

}
