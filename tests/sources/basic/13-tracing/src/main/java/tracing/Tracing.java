package tracing;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;


public class Tracing {

    public static void main(String[] args) {
        // Get Execution Parameters
        if (args.length != 1) {
            System.out.println("[ERROR] Bad call. Usage: tracing <numTasks>");
            System.exit(-1);
        }
        int numberTasks = Integer.parseInt(args[0]);

        System.out.println("[LOG] Number of tasks created: " + String.valueOf(numberTasks));

        // Creating tasks 1
        System.out.println("[LOG] Creating tasks type 1");
        for (int i = 0; i < numberTasks; ++i) {
            TracingImpl.task1();
        }

        // Creating tasks 2
        System.out.println("[LOG] Creating tasks type 2");
        for (int i = 0; i < numberTasks; ++i) {
            TracingImpl.task2();
        }

        // Creating tasks 3
        System.out.println("[LOG] Creating tasks type 3");
        for (int i = 0; i < numberTasks; ++i) {
            TracingImpl.task3();
        }

        // Creating and executing task 4
        System.out.println("[LOG] Creating tasks type 4");

        String counterName = "counter";
        int initialValue = 0;

        // Write value
        try {
            FileOutputStream fos = new FileOutputStream(counterName);
            fos.write(initialValue);
            System.out.println("Initial counter value is " + initialValue);
            fos.close();
        } catch (IOException ioe) {
            ioe.printStackTrace();
            System.exit(-1);
        }
        Integer count = new Integer(0);
        // Execute increment
        for (int i = 0; i < numberTasks; ++i) {
            count = TracingImpl.task4(counterName);
        }

        // Write new value
        try {
            FileInputStream fis = new FileInputStream(counterName);
            System.out.println("New CounterName: " + counterName.toLowerCase() + " returned Counter) " + count);
            System.out.println("Final counter value is " + fis.read());
            fis.close();
        } catch (IOException ioe) {
            ioe.printStackTrace();
            System.exit(-1);
        }

        System.out.println("Deleting created file");
        // Delete
        try {
            File file = new File(counterName);
            file.delete();
            System.out.println("Succesfully deleted counter file");
        } catch (Exception e) {
            System.out.println("Error deleting file");
            e.printStackTrace();
        }

        System.out.println("[LOG] All tasks created.");
        System.out.println("[LOG] No more jobs for main. Waiting all tasks to finish.");
    }

}
