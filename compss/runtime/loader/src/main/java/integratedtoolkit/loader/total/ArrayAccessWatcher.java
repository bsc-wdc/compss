package integratedtoolkit.loader.total;

public class ArrayAccessWatcher {

    /*
     * For now, there is no need to do anything inside this methods but to return or modify the array position.
     * The black-box synchronization will get us the right array before we reach the code of these methods.
     * 
     * TODO: Do not depend on the black-box, but deal directly with the object registry and distinguish 
     * between read and write access
     */
    public static void arrayWriteObject(Object array, int index, Object value) throws Throwable {
        // System.out.println("OBJECT ARRAY WRITE, index " + index + ", value " + value);
        ((Object[]) array)[index] = value;
    }

    public static void arrayWriteInt(Object array, int index, int value) throws Throwable {
        // System.out.println("INT ARRAY WRITE, index " + index + ", value " + value);
        ((int[]) array)[index] = value;
    }

    public static void arrayWriteByteOrBoolean(Object array, int index, byte value) throws Throwable {
        if (array instanceof boolean[]) {
            boolean boolValue = value == 0 ? false : true;
            ((boolean[]) array)[index] = boolValue;
        } else {
            ((byte[]) array)[index] = value;
        }
    }

    public static void arrayWriteChar(Object array, int index, char value) throws Throwable {
        ((char[]) array)[index] = value;
    }

    public static void arrayWriteDouble(Object array, int index, double value) throws Throwable {
        ((double[]) array)[index] = value;
    }

    public static void arrayWriteShort(Object array, int index, short value) throws Throwable {
        ((short[]) array)[index] = value;
    }

    public static void arrayWriteFloat(Object array, int index, float value) throws Throwable {
        ((float[]) array)[index] = value;
    }

    public static void arrayWriteLong(Object array, int index, long value) throws Throwable {
        ((long[]) array)[index] = value;
    }

    public static Object arrayReadObject(Object array, int index) throws Throwable {
        // System.out.println("OBJECT ARRAY READ, index " + index);
        return ((Object[]) array)[index];
    }

    public static int arrayReadInt(Object array, int index) throws Throwable {
        // System.out.println("INT ARRAY READ, index " + index);
        return ((int[]) array)[index];
    }

    public static byte arrayReadByteOrBoolean(Object array, int index) throws Throwable {
        if (array instanceof boolean[]) {
            boolean b = ((boolean[]) array)[index];
            if (b)
                return 1;
            else
                return 0;
        } else {
            return ((byte[]) array)[index];
        }
    }

    public static char arrayReadChar(Object array, int index) throws Throwable {
        return ((char[]) array)[index];
    }

    public static double arrayReadDouble(Object array, int index) throws Throwable {
        return ((double[]) array)[index];
    }

    public static float arrayReadFloat(Object array, int index) throws Throwable {
        return ((float[]) array)[index];
    }

    public static long arrayReadLong(Object array, int index) throws Throwable {
        return ((long[]) array)[index];
    }

    public static short arrayReadShort(Object array, int index) throws Throwable {
        return ((short[]) array)[index];
    }

}
