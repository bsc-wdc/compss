package basic;

public class BasicGraphImpl {

    public static void inTask() {
        System.out.println("HELLO");
    }

    public static void inTask(boolean token) {
        System.out.println(token);
    }

    public static void inTask(Integer token) {
        System.out.println(token);
    }

    public static void inoutTask(Integer token) {
        System.out.println(token);
    }

    public static Integer outTask() {
        System.out.println("BYE");
        return 30;
    }

}
