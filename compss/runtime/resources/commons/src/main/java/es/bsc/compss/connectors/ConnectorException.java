package es.bsc.compss.connectors;

public class ConnectorException extends Exception {

    /**
     * Exception Version UID are 2L in all Runtime
     */
    private static final long serialVersionUID = 2L;


    public ConnectorException(String message) {
        super(message);
    }

    public ConnectorException(Exception e) {
        super(e);
    }

    public ConnectorException(String msg, Exception e) {
        super(msg, e);
    }
}
