package integratedtoolkit.types;

public class OptimizationElement<T> implements Comparable<OptimizationElement> {

    private final T element;
    private long expectedStart;

    public OptimizationElement(T element, long expectedStart) {
        this.element = element;
        this.expectedStart = expectedStart;
    }

    @Override
    public int compareTo(OptimizationElement o) {
        return (int) (expectedStart - o.expectedStart);
    }

    public T getElement() {
        return element;
    }

}
