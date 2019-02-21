package objectTest;

public class ObjectTestImpl {

    public static void printObjects(A a, B b) {
        System.out.println("A int: " + a.getIntField());
        System.out.println("B string: " + b.getStringField());
    }

    public static void updateObjects(A a, B b, int i, String s) {
        int oldInt = a.getIntField();
        a.setIntField(oldInt + i);

        String oldString = b.getStringField();
        b.setStringField(oldString + s);
    }

    public static C createCObject(int i, String s) {
        System.out.println("Creating C with " + i + " and " + s);
        return new C(new A(i), new B(s));
    }

    public static Integer createInteger(int i) {
        return new Integer(i * i * i);
    }

    public static String createString(String s) {
        return s.concat(s).concat(s);
    }

    public static void printContent(Integer i, String s) {
        System.out.println("Integer: " + i);
        System.out.println("String: " + s);
    }

    public static int[] createIntArray(int value) {
        return new int[] { value, 2 * value, 3 * value };
    }

    public static A[][] createObjectArray(int value) {
        A a1 = new A(value);
        A a2 = new A(2 * value);
        A[][] aArray = new A[1][2];
        aArray[0][0] = a1;
        aArray[0][1] = a2;
        return aArray;
    }

    public static void printArrays(int[] array1, A[][] array2) {
        System.out.print("First array: ");
        for (int i = 0; i < array1.length; i++) {
            System.out.print(array1[i] + " ");
        }
        System.out.print("Second array: ");
        for (int i = 0; i < array2.length; i++) {
            A[] inner = array2[i];
            for (int j = 0; j < inner.length; j++)
                System.out.print(inner[j] + " ");
        }
    }

}
