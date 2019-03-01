package multiImplementations;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;


public class RealImplementation {

    private static final int SLEEP_TASK = 250; // ms


    public static void increment(String counterFile) {
        // Read value
        int count = 0;
        try (BufferedReader reader = new BufferedReader(new FileReader(counterFile))) {
            String strVal = reader.readLine();
            count = Integer.valueOf(strVal);
        } catch (IOException ioe) {
            ioe.printStackTrace();
            System.exit(-1);
        }

        // Write updated value
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(counterFile))) {
            writer.write(String.valueOf(++count));
            writer.flush();
        } catch (IOException ioe) {
            ioe.printStackTrace();
            System.exit(-1);
        }

        // Add delay to make task last longer
        try {
            Thread.sleep(SLEEP_TASK);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

}
