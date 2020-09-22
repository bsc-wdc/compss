package files;

import es.bsc.compss.api.COMPSs;
import utils.FileContentManagement;


public class MainImpl {

    public static void taskIn(String fileName) {
        int v = FileContentManagement.readValueFromFile(fileName);
        System.out.println("File: " + fileName + " , Value: " + v);
    }

    public static void taskInNested(String fileName) {
        System.out.println("TaskNested IN with file: " + fileName);
        for (int i = 0; i < Main.NUM_NESTED_TASKS; ++i) {
            taskIn(fileName);
        }
        COMPSs.barrier();
    }

    public static void taskOut(String fileName) {
        // Write
        int newValue = 2;
        FileContentManagement.writeValueToFile(fileName, newValue);
    }

    public static void taskOutNested(String fileName) {
        System.out.println("TaskNested OUT with file: " + fileName);
        for (int i = 0; i < Main.NUM_NESTED_TASKS; ++i) {
            taskOut(fileName);
        }
        COMPSs.barrier();
    }

    public static void taskInout(String fileName) {
        // Read
        int v = FileContentManagement.readValueFromFile(fileName);
        System.out.println("File: " + fileName + " , Value: " + v);

        // Write
        int newValue = 2;
        FileContentManagement.writeValueToFile(fileName, newValue);
    }

    public static void taskInoutNested(String fileName) {
        System.out.println("TaskNested INOUT with file: " + fileName);
        for (int i = 0; i < Main.NUM_NESTED_TASKS; ++i) {
            taskInout(fileName);
        }
        COMPSs.barrier();
    }

}
