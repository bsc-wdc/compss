package simpleTests;

import utils.GenericObject;
import utils.TasksImplementation;


public class SingleChain {

    private static final int N = 1;


    public static void main(String[] args) {
        GenericObject[] values = new GenericObject[N];

        for (int i = 0; i < N; ++i) {
            values[i] = TasksImplementation.initialize();
        }

        for (int i = 0; i < N; ++i) {
            TasksImplementation.increment(values[i]);
        }

        // Final sync
        for (int i = 0; i < N; ++i) {
            System.out.println("Value " + i + " is " + values[i].getValue());
        }
    }

}
