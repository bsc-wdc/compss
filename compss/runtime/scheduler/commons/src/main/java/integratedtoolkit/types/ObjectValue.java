package integratedtoolkit.types;

public class ObjectValue<T> implements Comparable<ObjectValue<T>> {

    public T o;
    public Score value;

    public ObjectValue(T o, Score value) {
        this.o = o;
        this.value = value;
    }

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
