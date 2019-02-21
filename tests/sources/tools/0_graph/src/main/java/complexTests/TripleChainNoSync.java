package complexTests;

import utils.GenericObject;
import utils.TasksImplementation;


public class TripleChainNoSync {

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
    }

}
