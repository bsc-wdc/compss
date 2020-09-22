package files;

import es.bsc.compss.api.COMPSs;

import utils.FileContentManagement;


public class Main {

    protected static final int NUM_TASKS = 2;
    protected static final int NUM_NESTED_TASKS = 2;


    private static void testIn() {
        System.out.println("[DEBUG] Test IN Files");

        System.out.println("[DEBUG] - Normal tasks");
        final String baseFileNameNormal = "file_in_normal_";
        for (int i = 0; i < NUM_TASKS; ++i) {
            String fileName = baseFileNameNormal + i;
            FileContentManagement.writeValueToFile(fileName, 1);
            MainImpl.taskIn(fileName);
        }
        COMPSs.barrier();

        System.out.println("[DEBUG] - Nested tasks");
        final String baseFileNameNested = "file_in_nested_";
        for (int i = 0; i < NUM_TASKS; ++i) {
            String fileName = baseFileNameNested + i;
            FileContentManagement.writeValueToFile(fileName, 1);
            MainImpl.taskInNested(fileName);
        }
        COMPSs.barrier();

        System.out.println("[DEBUG] - Combined tasks");
        final String baseFileNameHybrid1 = "file_in_hybrid_normal_";
        final String baseFileNameHybrid2 = "file_in_hybrid_nested_";
        for (int i = 0; i < NUM_TASKS; ++i) {
            String fileName1 = baseFileNameHybrid1 + i;
            FileContentManagement.writeValueToFile(fileName1, 1);
            MainImpl.taskIn(fileName1);
            String fileName2 = baseFileNameHybrid2 + i;
            FileContentManagement.writeValueToFile(fileName2, 1);
            MainImpl.taskInNested(fileName2);
        }
        COMPSs.barrier();

        COMPSs.barrier();
        System.out.println("[DEBUG] Test IN Files DONE");
    }

    private static void testOut() {
        System.out.println("[DEBUG] Test OUT Files");

        System.out.println("[DEBUG] - Normal tasks");
        final String baseFileNameNormal = "file_out_normal_";
        for (int i = 0; i < NUM_TASKS; ++i) {
            String fileName = baseFileNameNormal + i;
            MainImpl.taskOut(fileName);
        }
        for (int i = 0; i < NUM_TASKS; ++i) {
            String fileName = baseFileNameNormal + i;
            int v = FileContentManagement.readValueFromFile(fileName);
            System.out.println("[DEBUG]   - File: " + fileName + " , Value: " + v);
        }
        COMPSs.barrier();

        System.out.println("[DEBUG] - Nested tasks");
        final String baseFileNameNested = "file_out_nested_";
        for (int i = 0; i < NUM_TASKS; ++i) {
            String fileName = baseFileNameNested + i;
            MainImpl.taskOutNested(fileName);
        }
        for (int i = 0; i < NUM_TASKS; ++i) {
            String fileName = baseFileNameNormal + i;
            int v = FileContentManagement.readValueFromFile(fileName);
            System.out.println("[DEBUG]   - File: " + fileName + " , Value: " + v);
        }
        COMPSs.barrier();

        System.out.println("[DEBUG] - Combined tasks");
        final String baseFileNameHybrid1 = "file_out_hybrid_normal_";
        final String baseFileNameHybrid2 = "file_out_hybrid_nested_";
        for (int i = 0; i < NUM_TASKS; ++i) {
            String fileName1 = baseFileNameHybrid1 + i;
            MainImpl.taskOut(fileName1);
            String fileName2 = baseFileNameHybrid2 + i;
            MainImpl.taskOutNested(fileName2);
        }
        for (int i = 0; i < NUM_TASKS; ++i) {
            String fileName1 = baseFileNameHybrid1 + i;
            String fileName2 = baseFileNameHybrid2 + i;
            int v1 = FileContentManagement.readValueFromFile(fileName1);
            int v2 = FileContentManagement.readValueFromFile(fileName2);
            System.out.println("[DEBUG]   - File1: " + fileName1 + " , Value1: " + v1);
            System.out.println("[DEBUG]   - File2: " + fileName2 + " , Value2: " + v2);
        }
        COMPSs.barrier();

        COMPSs.barrier();
        System.out.println("[DEBUG] Test OUT Files DONE");
    }

    private static void testInToOutToIn() {
        System.out.println("[DEBUG] Test IN->OUT->IN Files");

        System.out.println("[DEBUG] - From IN task to OUT task to IN task");
        final String baseFileNameIn2Out2In = "file_in2out2in_";
        for (int i = 0; i < NUM_TASKS; ++i) {
            String fileName = baseFileNameIn2Out2In + i;
            FileContentManagement.writeValueToFile(fileName, 1);
            MainImpl.taskIn(fileName);
            MainImpl.taskOut(fileName);
            MainImpl.taskIn(fileName);
        }
        COMPSs.barrier();

        System.out.println("[DEBUG] - From IN task to OUT task to IN task");
        final String baseFileNameInNested2OutNested2InNested = "file_inNested2outNested2inNested_";
        for (int i = 0; i < NUM_TASKS; ++i) {
            String fileName = baseFileNameInNested2OutNested2InNested + i;
            FileContentManagement.writeValueToFile(fileName, 1);
            MainImpl.taskInNested(fileName);
            MainImpl.taskOutNested(fileName);
            MainImpl.taskInNested(fileName);
        }
        COMPSs.barrier();

        System.out.println("[DEBUG] - From IN task to OUT task to IN task");
        final String baseFileNameIn2OutNested2In = "file_in2outNested2in_";
        for (int i = 0; i < NUM_TASKS; ++i) {
            String fileName = baseFileNameIn2OutNested2In + i;
            FileContentManagement.writeValueToFile(fileName, 1);
            MainImpl.taskIn(fileName);
            MainImpl.taskOutNested(fileName);
            MainImpl.taskIn(fileName);
        }
        COMPSs.barrier();

        System.out.println("[DEBUG] - From IN task to OUT task to IN task");
        final String baseFileNameInNested2Out2InNested = "file_inNested2out2inNested_";
        for (int i = 0; i < NUM_TASKS; ++i) {
            String fileName = baseFileNameInNested2Out2InNested + i;
            FileContentManagement.writeValueToFile(fileName, 1);
            MainImpl.taskInNested(fileName);
            MainImpl.taskOut(fileName);
            MainImpl.taskInNested(fileName);
        }
        COMPSs.barrier();

        COMPSs.barrier();
        System.out.println("[DEBUG] Test IN->OUT->IN Files DONE");
    }

    private static void testInout() {
        System.out.println("[DEBUG] Test INOUT Files");

        System.out.println("[DEBUG] - Normal tasks from main");
        final String baseFileNameNormalMain = "file_inout_normal_main_";
        String[] fileNamesMain = new String[NUM_TASKS];
        for (int i = 0; i < NUM_TASKS; ++i) {
            fileNamesMain[i] = baseFileNameNormalMain + i;
            FileContentManagement.writeValueToFile(fileNamesMain[i], 1);
        }
        for (int i = 0; i < NUM_TASKS; ++i) {
            MainImpl.taskInout(fileNamesMain[i]);
        }
        for (int i = 0; i < NUM_TASKS; ++i) {
            int v = FileContentManagement.readValueFromFile(fileNamesMain[i]);
            System.out.println("[DEBUG]   - File: " + fileNamesMain[i] + " , Value: " + v);
        }
        COMPSs.barrier();

        System.out.println("[DEBUG] - Normal tasks from task");
        final String baseFileNameNormalTask = "file_inout_normal_task_";
        String[] fileNamesTask = new String[NUM_TASKS];
        for (int i = 0; i < NUM_TASKS; ++i) {
            fileNamesTask[i] = baseFileNameNormalTask + i;
            MainImpl.taskOut(fileNamesTask[i]);
        }
        for (int i = 0; i < NUM_TASKS; ++i) {
            MainImpl.taskInout(fileNamesTask[i]);
        }
        for (int i = 0; i < NUM_TASKS; ++i) {
            MainImpl.taskIn(fileNamesTask[i]);
        }
        COMPSs.barrier();

        System.out.println("[DEBUG] - Nested tasks from main");
        final String baseFileNameNestedMain = "file_inout_nested_main_";
        String[] fileNamesNestedMain = new String[NUM_TASKS];
        for (int i = 0; i < NUM_TASKS; ++i) {
            fileNamesNestedMain[i] = baseFileNameNestedMain + i;
            FileContentManagement.writeValueToFile(fileNamesNestedMain[i], 1);
        }
        for (int i = 0; i < NUM_TASKS; ++i) {
            MainImpl.taskInoutNested(fileNamesMain[i]);
        }
        for (int i = 0; i < NUM_TASKS; ++i) {
            int v = FileContentManagement.readValueFromFile(fileNamesNestedMain[i]);
            System.out.println("[DEBUG]   - File: " + fileNamesNestedMain[i] + " , Value: " + v);
        }
        COMPSs.barrier();

        System.out.println("[DEBUG] - Nested tasks from task");
        final String baseFileNameNestedTask = "file_inout_nested_task_";
        String[] fileNamesNestedTask = new String[NUM_TASKS];
        for (int i = 0; i < NUM_TASKS; ++i) {
            fileNamesNestedTask[i] = baseFileNameNestedTask + i;
            MainImpl.taskOut(fileNamesNestedTask[i]);
        }
        for (int i = 0; i < NUM_TASKS; ++i) {
            MainImpl.taskInoutNested(fileNamesNestedTask[i]);
        }
        for (int i = 0; i < NUM_TASKS; ++i) {
            MainImpl.taskIn(fileNamesNestedTask[i]);
        }
        COMPSs.barrier();

        System.out.println("[DEBUG] - Nested tasks from nested task");
        final String baseFileNameNestedTaskNested = "file_inout_nested_task_nested_";
        String[] fileNamesNestedTaskNested = new String[NUM_TASKS];
        for (int i = 0; i < NUM_TASKS; ++i) {
            fileNamesNestedTaskNested[i] = baseFileNameNestedTaskNested + i;
            MainImpl.taskOutNested(fileNamesNestedTaskNested[i]);
        }
        for (int i = 0; i < NUM_TASKS; ++i) {
            MainImpl.taskInoutNested(fileNamesNestedTaskNested[i]);
        }
        for (int i = 0; i < NUM_TASKS; ++i) {
            MainImpl.taskInNested(fileNamesNestedTaskNested[i]);
        }
        COMPSs.barrier();

        COMPSs.barrier();
        System.out.println("[DEBUG] Test INOUT Files DONE");
    }

    /**
     * Entry point.
     * 
     * @param args System arguments.
     */
    public static void main(String[] args) {
        // IN
        testIn();

        // OUT
        testOut();

        // IN 2 OUT 2 IN
        testInToOutToIn();

        // INOUT
        testInout();
    }
}
