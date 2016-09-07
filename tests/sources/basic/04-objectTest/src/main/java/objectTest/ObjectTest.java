package objectTest;

public class ObjectTest {

    public static void main(String args[]) throws Exception {
        testUserObjects();
        testImmutableObjects();
        testTargetObjects();
        testPrimitiveReturn();
        testConstructor();
        testArrays();
    }

    private static void testUserObjects() throws InterruptedException {
        System.out.println("[LOG] Test User Objects");
        C c = ObjectTestImpl.createCObject(7, "OPTIMIS");

        A a = c.getAField();
        B b = c.getBField();

        ObjectTestImpl.printObjects(a, b);
        Thread.sleep(500);
        ObjectTestImpl.updateObjects(a, b, 10, "-CLOUD");
        Thread.sleep(500);
        ObjectTestImpl.updateObjects(a, b, 3, "-PROJECT");

        // Synchronize by method call (target object of the method call)
        System.out.println("Updated A int: " + a.getIntField());

        // Synchronize by field access
        System.out.println("Updated B string: " + b.stringField);

        b.stringField = "OVERWRITE";

        ObjectTestImpl.printObjects(a, b);
        Thread.sleep(1000);

    }

    private static void testImmutableObjects() throws InterruptedException {
        System.out.println("[LOG] Test Immutable Objects");
        Integer i = ObjectTestImpl.createInteger(5);
        String s = ObjectTestImpl.createString("SDO");

        System.out.println("Integer value: " + i);
        System.out.println("String value: " + s);

        ObjectTestImpl.printContent(i, s);
        Thread.sleep(1000);
    }

    private static void testTargetObjects() {
        System.out.println("[LOG] Test Target Objects");
        A a = new A(32);
        B b = new B("Old value");

        System.out.println("Integer before: " + a.getIntField());
        System.out.println("String before: " + b.getStringField());

        Integer oldValue = a.getAndSetIntField(64);
        b.setStringField("New value");

        System.out.println("Integer after: " + a.getIntField() + ", old value was " + oldValue.intValue());
        System.out.println("String after: " + b.getStringField());
    }

    private static void testPrimitiveReturn() {
        System.out.println("[LOG] Test Primitive Return");
        A a = new A(4);
        int i = a.square();
        System.out.println("Square of " + a.getIntField() + " is " + i);
        a.setIntField(5);
        System.out.println("Square of " + a.getIntField() + " is " + a.square());
    }

    private static void testConstructor() {
        System.out.println("[LOG] Test Constructor");
        A a = new A(3);
        B b = new B("CONSTRUCTOR");

        ObjectTestImpl.updateObjects(a, b, 3, " TEST");

        C c = new C(a, b);

        System.out.println("In app, A is " + c.getAField().getIntField() + " and B is " + c.getBField().getStringField());
    }

    private static void testArrays() {
        System.out.println("[LOG] Test Arrays");
        int[] array = ObjectTestImpl.createIntArray(9);
        A[][] matrix = ObjectTestImpl.createObjectArray(11);
        ObjectTestImpl.printArrays(array, matrix);

        // int[] i = ObjectTestImpl.createIntArray(9);
        // System.out.println("Length: " + i.length);

        System.out.println("Element in position 1 is " + array[1]);
        System.out.println("Element in position 0,1 is " + matrix[0][1].getIntField());

        array[1] = 99;
        matrix[0][1] = new A(99);

        ObjectTestImpl.printArrays(array, matrix);
    }

}
