package streams;

import streams.components.COMPSsStream;
import streams.components.Consumer;
import streams.components.Producer;
import streams.exceptions.ConsumerException;
import streams.exceptions.ProducerException;
import streams.types.Result;


public class Main {

    private static final int WAIT_FOR_RUNTIME = 5_000; // ms


    public static void main(String[] args) throws ProducerException, ConsumerException {
        // Check and get parameters
        if (args.length != 1) {
            System.out.println("[ERROR] Bad number of parameters");
            System.out.println("    Usage: streams.Main <numMessages>");
            System.exit(-1);
        }
        int numMessages = Integer.parseInt(args[0]);

        // ------------------------------------------------------------------------
        // Add a sleep to wait for both workers to be ready
        try {
            Thread.sleep(WAIT_FOR_RUNTIME);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // ------------------------------------------------------------------------
        // Create stream
        COMPSsStream stream = new COMPSsStream();

        // Launch producer task
        Integer exitP = Producer.sendMessages(stream, numMessages);

        // Launch consumer task
        Result resultC = Consumer.receiveMessages(stream);

        // Synchronize
        if (exitP != 0) {
            throw new ProducerException("ERROR: Producer ended with exitValue " + exitP);
        }
        Integer exitC = resultC.getExitValue();
        if (exitC != 0) {
            throw new ConsumerException("ERROR: Consumer ended with exitValue " + exitC);
        }

        System.out.println("Consumer has returned:");
        System.out.println("- NumMessages = " + resultC.getNumMessages());
        System.out.println("- AcumValue = " + resultC.getAccumValue());
    }

}
