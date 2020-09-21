package objectDeregister;

public class ClassInstanceTest {

    static {
        System.loadLibrary("agent");
    }


    public static native int countInstances(Class<?> klass);

}
