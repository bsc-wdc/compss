package testHttpTask;

import java.io.*;


public class TestHttpTask {

    private static final String FILE_NAME = "text.txt";


    public static void main(String[] args) throws Exception {
        System.out.println("[LOG] Main program started.");
        testGetRequests();
        testPostRequests();
    }

    private static void testGetRequests() {
        // GET REQUEST TESTS
        TestHttpTaskImpl http = new TestHttpTaskImpl();
        http.testGet();
        assertAndPrint("GET: dummy", true);

        String message = "holala";
        int len = http.testGetLength(message);
        assertAndPrint("GET: getLength", len == message.length());

        String res = http.testProducesString(message);
        assertAndPrint("GET: producesString", message.equals(res));

        String ret = http.testNestedProduces(message);
        assertAndPrint("GET: testNestedProduces", ret.equals(message));

    }

    private static void testPostRequests() throws IOException {
        // POST REQUEST TESTS
        TestHttpTaskImpl http = new TestHttpTaskImpl();
        String res = http.testPost();
        assertAndPrint("POST: dummy", res.equals("post_works"));

        String message = "holala";
        String payload = http.testPayloadWithParam(message);
        assertAndPrint("POST: testPayload", message.equals(payload));

        newFile();
        writeInFile();
        String ret = http.testPayloadWithFileParam(FILE_NAME);
        assertAndPrint("POST: testFileParam", ret.equals("testing"));

    }

    private static void writeInFile() {
        String str = "testing";
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(FILE_NAME, true))) {
            writer.write(str);
        } catch (IOException fnfe) {
            fnfe.printStackTrace();
            System.exit(-1);
        }
    }

    private static void newFile() throws IOException {
        File file = new File(FILE_NAME);
        // Delete previous occurrences of the file
        if (file.exists()) {
            file.delete();
        }
        // Create the file and directories if required
        boolean createdFile = file.createNewFile();
        if (!createdFile) {
            throw new IOException("[ERROR] Cannot create test file");
        }
    }

    private static void assertAndPrint(String testName, boolean condition) {
        if (condition) {
            System.out.println("TEST PASSED:" + testName);
        } else {
            System.out.println("TEST FAILED:" + testName);
        }
    }

}
