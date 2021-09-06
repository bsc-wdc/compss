package testHttpTask;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;


public class TestHttpTask {

    public static void main(String[] args) throws Exception {
        System.out.println("[LOG] Main program started.");
        testGetRequests();
        testPostRequests();
        System.out.println("[LOG] Main program finished.");
    }

    private static void testPostRequests() {
        // POST REQUEST TESTS
        TestHttpTaskImpl http = new TestHttpTaskImpl();
        String res = http.testPost();
        assertAndPrint("POST: dummy", res.equals("post_works"));

        String message = "holala";
        String payload = http.testPayloadWithParam(message);
        assertAndPrint("POST: testPayload", message.equals(payload));

        String f = "test_file";
        FileOutputStream fos = null;
        try {
            fos = new FileOutputStream(f);
            fos.write(1992);
            fos.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        // int ret = http.testPayloadWithFileParam(f);
        // assertAndPrint("POST: testPayloadWithFileParam", ret == 1992);

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

    private static void assertAndPrint(String testName, boolean condition) {
        if (condition) {
            System.out.println("TEST PASSED:" + testName);
        } else {
            System.out.println("TEST FAILED:" + testName);
        }
    }

}
