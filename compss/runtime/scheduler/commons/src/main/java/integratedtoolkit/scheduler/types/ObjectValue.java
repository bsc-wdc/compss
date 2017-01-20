package integratedtoolkit.scheduler.types;

public class ObjectValue<T> implements Comparable<ObjectValue<T>> {

    private final T obj;
    private final Score value;


    public ObjectValue(T o, Score value) {
        this.obj = o;
        this.value = value;
    }

    public T getObject() {
        return this.obj;
    }

    public Score getScore() {
        return this.value;
    }

    @Override
    public int hashCode() {
        return obj.hashCode();
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof ObjectValue) {
            ObjectValue<T> ov = (ObjectValue<T>) obj;
            return this.hashCode() == ov.hashCode();
        }

        return false;
    }

    @Override
    public int compareTo(ObjectValue<T> o) {
        if (Score.isBetter(this.value, o.value)) {
            return -1;
        } else if (Score.isBetter(o.value, this.value)) {
            return 1;
        } else {
            return 0;
        }
    }
    
}
