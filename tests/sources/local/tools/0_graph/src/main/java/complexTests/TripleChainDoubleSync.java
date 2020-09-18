package complexTests;

import utils.GenericObject;
import utils.TasksImplementation;


public class TripleChainDoubleSync {

    private static final int N = 3;
    private static final int D = 3;


    public static void main(String[] args) {
        GenericObject[] values = new GenericObject[N];

        for (int i = 0; i < N; ++i) {
            values[i] = TasksImplementation.initialize();
        }

        for (int d = 0; d < D; ++d) {
            for (int i = 0; i < N; ++i) {
                TasksImplementation.increment(values[i]);
            }
        }

        // Sync
        for (int i = 0; i < N; ++i) {
            System.out.println("Value " + i + " is " + values[i].getValue());
        }

        for (int d = 0; d < D; ++d) {
            for (int i = 0; i < N; ++i) {
                TasksImplementation.increment(values[i]);
            }
        }

        // Sync 2
        for (int i = 0; i < N; ++i) {
            System.out.println("Final Value " + i + " is " + values[i].getValue());
        }
    }

}
