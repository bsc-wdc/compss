package es.bsc.compss.nio.commands;

public abstract class RetriableCommand implements Command {

    private static int MAX_RETRIES = 3;
    private int retries = 0;


    public void increaseRetries() {
        retries++;
    }

    public boolean canRetry() {
        return (retries < MAX_RETRIES);
    }

}
