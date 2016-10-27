package com.bsc.compss.exceptions;

public class NonInstantiableException extends RuntimeException {

    /**
     * Exceptions Version UID are 2L in all Runtime
     */
    private static final long serialVersionUID = 2L;


    public NonInstantiableException(String className) {
        super("Class " + className + " can not be instantiated.");
    }

}
