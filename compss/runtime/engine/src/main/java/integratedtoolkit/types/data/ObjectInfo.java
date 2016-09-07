package integratedtoolkit.types.data;

public class ObjectInfo extends DataInfo {

    // Hash code of the object

    private int code;


    public ObjectInfo(int code) {
        super();
        this.code = code;
    }

    public int getCode() {
        return code;
    }

}
