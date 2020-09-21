
package workerinmaster;

import es.bsc.compss.api.COMPSs;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.util.Comparator;
import java.util.TreeSet;


public class Test {

    public static final int PARALLEL_TEST_COUNT = 20;
    public static final int PARALLEL_TEST_MAX_COUNT = 4;

    private static final String INITIAL_CONTENT = "This is the initial content of the file";
    private static final String UPDATED_CONTENT_1 = "This is the updated content 1 of the file";
    private static final String UPDATED_CONTENT_2 = "This is the updated content 2 of the file";
    private static final String UPDATED_CONTENT_3 = "This is the updated content 3 of the file";
    private static final String UPDATED_CONTENT_4 = "This is the updated content 4 of the file";
    private static final String UPDATED_CONTENT_5 = "This is the updated content 5 of the file";


    private static void verifyLine(String obtained, String expected) throws Exception {
        if (obtained != null) {
            if (expected == null || obtained.compareTo(expected) != 0) {
                System.err.println("Expecting:\n" + expected + "\n and obtained\n" + obtained);
                throw new Exception("Unexpected file content.");
            }
        } else {
            if (expected != null) {
                System.err.println("Expecting:\n" + expected + "\n and obtained\n" + obtained);
                throw new Exception("Unexpected file content.");
            }
        }
    }

    private static void basicTypesTest() throws Exception {

        System.out.println("Running basic types task");

        // Run basic types test
        String fileName = "basic_types_file";
        boolean b = true;
        char c = 'E';
        String s = "My Test";
        byte by = 7;
        short sh = 77;
        int i = 777;
        long l = 7777;
        float f = 7.7f;
        double d = 7.77777d;

        Tasks.testBasicTypes(fileName, b, c, s, by, sh, i, l, f, d);

        try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
            String line;
            line = br.readLine();
            verifyLine(line, "TEST BASIC TYPES");
            line = br.readLine();
            verifyLine(line, "- boolean: " + b);
            line = br.readLine();
            verifyLine(line, "- char: " + c);
            line = br.readLine();
            verifyLine(line, "- String: " + s);
            line = br.readLine();
            verifyLine(line, "- byte: " + by);
            line = br.readLine();
            verifyLine(line, "- short: " + sh);
            line = br.readLine();
            verifyLine(line, "- int: " + i);
            line = br.readLine();
            verifyLine(line, "- long: " + l);
            line = br.readLine();
            verifyLine(line, "- float: " + f);
            line = br.readLine();
            verifyLine(line, "- double: " + d);
            line = br.readLine();
            verifyLine(line, null);
        }
        COMPSs.barrier();
        new File(fileName).delete();
        System.out.println("\t OK");
    }

    private static void mainToTaskTest() throws Exception {
        System.out.println("Creating file on main and using it on task");

        String fileName = "main_to_task_file";
        try (FileOutputStream fos = new FileOutputStream(fileName, true)) {
            String value = INITIAL_CONTENT + "\n";
            fos.write(value.getBytes());
            fos.close();
        }
        Tasks.checkFileWithContent(INITIAL_CONTENT, fileName);
        COMPSs.barrier();
        new File(fileName).delete();
        System.out.println("\t OK");
    }

    private static void taskToMainTest() throws Exception {
        System.out.println("Creating file on task and using it on main");
        String fileName = "task_to_main_file";
        Tasks.createFileWithContent(INITIAL_CONTENT, fileName);
        try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
            String line;
            line = br.readLine();
            verifyLine(line, INITIAL_CONTENT);
            line = br.readLine();
            verifyLine(line, null);
        }
        COMPSs.barrier();
        new File(fileName).delete();
        System.out.println("\t OK");
    }

    private static void fileDependenciesTest() throws Exception {
        System.out.println("Testing file dependencies");
        String fileName = "dependencies_file_1";

        Tasks.createFileWithContent(INITIAL_CONTENT, fileName);
        Tasks.checkFileWithContent(INITIAL_CONTENT, fileName);
        Tasks.checkAndUpdateFileWithContent(INITIAL_CONTENT, UPDATED_CONTENT_1, fileName);
        try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
            String line;
            line = br.readLine();
            verifyLine(line, UPDATED_CONTENT_1);
        }
        COMPSs.barrier();
        new File(fileName).delete();
        System.out.println("\t OK");
    }

    private static void fileDependenciesTestComplex() throws Exception {
        System.out.println("Testing file dependencies - Complex Version");
        String fileName = "dependencies_file_2";

        Tasks.createFileWithContent(INITIAL_CONTENT, fileName);
        Tasks.checkFileWithContent(INITIAL_CONTENT, fileName);
        Tasks.checkAndUpdateFileWithContent(INITIAL_CONTENT, UPDATED_CONTENT_1, fileName);
        try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
            String line;
            line = br.readLine();
            verifyLine(line, UPDATED_CONTENT_1);
        }
        // Update File Content on Main
        try (FileOutputStream fos = new FileOutputStream(fileName, false)) {
            String value = UPDATED_CONTENT_2 + "\n";
            fos.write(value.getBytes());
            fos.close();
        }

        // Verify File update on Main
        try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
            String line;
            line = br.readLine();
            verifyLine(line, UPDATED_CONTENT_2);
        }
        Tasks.checkFileWithContent(UPDATED_CONTENT_2, fileName);

        Tasks.checkAndUpdateFileWithContent(UPDATED_CONTENT_2, UPDATED_CONTENT_3, fileName);
        Tasks.checkFileWithContent(UPDATED_CONTENT_3, fileName);
        Tasks.checkAndUpdateFileWithContent(UPDATED_CONTENT_3, UPDATED_CONTENT_4, fileName);
        Tasks.checkAndUpdateFileWithContent(UPDATED_CONTENT_4, UPDATED_CONTENT_5, fileName);
        try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
            String line;
            line = br.readLine();
            verifyLine(line, UPDATED_CONTENT_5);
        }
        COMPSs.barrier();
        new File(fileName).delete();
        System.out.println("\t OK");
    }

    private static void objectDependenciesTest() throws Exception {
        System.out.println("Testing object dependencies");
        StringWrapper sw = Tasks.createObjectWithContent(INITIAL_CONTENT);
        Tasks.checkObjectWithContent(INITIAL_CONTENT, sw);
        Tasks.checkAndUpdateObjectWithContent(INITIAL_CONTENT, UPDATED_CONTENT_1, sw);
        String line;
        line = sw.getValue();
        verifyLine(line, UPDATED_CONTENT_1);
        COMPSs.barrier();
        System.out.println("\t OK");
    }

    private static void objectDependenciesTestComplex() throws Exception {
        System.out.println("Testing object dependencies - Complex Version");

        StringWrapper sw = Tasks.createObjectWithContent(INITIAL_CONTENT);
        Tasks.checkObjectWithContent(INITIAL_CONTENT, sw);
        Tasks.checkAndUpdateObjectWithContent(INITIAL_CONTENT, UPDATED_CONTENT_1, sw);

        String line = sw.getValue();
        verifyLine(line, UPDATED_CONTENT_1);

        // Update object Content on Main
        sw.setValue(UPDATED_CONTENT_2);

        // Verify object update on Main
        line = sw.getValue();
        verifyLine(line, UPDATED_CONTENT_2);

        // Verify Object content on task
        Tasks.checkObjectWithContent(UPDATED_CONTENT_2, sw);

        // Update value on task
        Tasks.checkAndUpdateObjectWithContent(UPDATED_CONTENT_2, UPDATED_CONTENT_3, sw);
        // Check proper value on task
        Tasks.checkObjectWithContent(UPDATED_CONTENT_3, sw);
        // Update twice on tasks
        Tasks.checkAndUpdateObjectWithContent(UPDATED_CONTENT_3, UPDATED_CONTENT_4, sw);
        Tasks.checkAndUpdateObjectWithContent(UPDATED_CONTENT_4, UPDATED_CONTENT_5, sw);

        // Verify object update on Main
        line = sw.getValue();
        verifyLine(line, UPDATED_CONTENT_5);

        COMPSs.barrier();
        System.out.println("\t OK");
    }

    private static void maxTaskAtATimeTest() throws Exception {
        System.out.println("Testing concurrent executions");
        Report[] reports;
        reports = new Report[PARALLEL_TEST_COUNT];
        for (int i = 0; i < PARALLEL_TEST_COUNT; i++) {
            reports[i] = Tasks.sleepTask();
        }
        COMPSs.barrier();

        // Verify the number of tasks able to run in parallel in the process
        TreeSet<WorkerModification> modifications;
        modifications = new TreeSet<>(new Comparator<WorkerModification>() {

            @Override
            public int compare(WorkerModification t1, WorkerModification t2) {
                int result = Long.compare(t1.getTime(), t2.getTime());
                if (result == 0) {
                    result = Integer.compare(t2.getChange(), t1.getChange());
                }
                if (result == 0) {
                    result = Integer.compare(t2.hashCode(), t1.hashCode());
                }
                return result;
            }
        });
        for (Report r : reports) {
            WorkerModification startMod = new WorkerModification(r.getStartTime(), 1);
            WorkerModification endMod = new WorkerModification(r.getEndTime(), -1);
            modifications.add(startMod);
            modifications.add(endMod);
        }

        int currentCount = 0;
        int maxCount = 0;
        for (WorkerModification m : modifications) {
            currentCount += m.getChange();
            maxCount = Math.max(maxCount, currentCount);
            if (currentCount > PARALLEL_TEST_MAX_COUNT) {
                throw new Exception("Scheduling does not properly manage the maximum number of tasks assigned");
            }
        }
        if (maxCount != PARALLEL_TEST_MAX_COUNT) {
            throw new Exception("Worker in master does not hold as many task as possible");
        }
        System.out.println("\tOK");
    }

    /**
     * Main method of the test. It validates the worker in the master process.
     *
     * @param args ignored!
     * @throws Exception Unexpected result obtained during the execution
     */
    public static void main(String[] args) throws Exception {
        basicTypesTest();
        mainToTaskTest();
        taskToMainTest();
        fileDependenciesTest();
        fileDependenciesTestComplex();
        objectDependenciesTest();
        objectDependenciesTestComplex();
        maxTaskAtATimeTest();
    }

}
