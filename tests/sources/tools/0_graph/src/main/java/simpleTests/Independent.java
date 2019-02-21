package simpleTests;

import utils.GenericObject;
import utils.TasksImplementation;


public class Independent {

    private static final int N = 3;


    public static void main(String[] args) {
        GenericObject[] values = new GenericObject[N];

        for (int i = 0; i < N; ++i) {
            values[i] = TasksImplementation.initialize();
        }
    }

}
