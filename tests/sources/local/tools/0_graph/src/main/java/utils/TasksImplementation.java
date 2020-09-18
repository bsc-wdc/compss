package utils;

public class TasksImplementation {

    /**
     * Initializes the genric object to 0
     * 
     * @return
     */
    public static GenericObject initialize() {
        GenericObject go = new GenericObject(0);
        return go;
    }

    /**
     * Increments the generic object's value by 1
     * 
     * @param go
     */
    public static void increment(GenericObject go) {
        Integer val = go.getValue() + 1;
        go.setValue(val);
    }

}
