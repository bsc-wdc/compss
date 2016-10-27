package integratedtoolkit.types;

public class ObjectValue<T> implements Comparable<ObjectValue<T>> {

    private final T o;
    private final Score value;


    public ObjectValue(T o, Score value) {
        this.o = o;
        this.value = value;
    }

    public T getObject() {
        return this.o;
    }

    public Score getScore() {
        return this.value;
    }

    @Override
    public int compareTo(ObjectValue<T> o) {
        if (Score.isBetter(value, o.value)) {
            return -1;
        } else {
            if (Score.isBetter(o.value, o.value)) {
                return 1;
            } else {
                return 0;
            }
        }

    }
}
