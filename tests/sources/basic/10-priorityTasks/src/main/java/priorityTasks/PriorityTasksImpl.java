package priorityTasks;

import java.io.FileOutputStream;
import java.io.IOException;


public class PriorityTasksImpl {

    public static void normalTask(String fileName) {
        System.out.println("Executing normal task.");
        int value = 1;
        try {
            FileOutputStream fos = new FileOutputStream(fileName, true);
            fos.write(value);
            fos.close();
        } catch (IOException ioe) {
            ioe.printStackTrace();
            System.exit(-1);
        }
    }

    public static void priorityTask(String fileName) {
        System.out.println("Executing priority task.");
        int value = 2;
        try {
            FileOutputStream fos = new FileOutputStream(fileName, true);
            fos.write(value);
            fos.close();
        } catch (IOException ioe) {
            ioe.printStackTrace();
            System.exit(-1);
        }
    }
}
