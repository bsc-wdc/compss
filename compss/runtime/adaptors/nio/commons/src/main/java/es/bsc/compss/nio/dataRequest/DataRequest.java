package es.bsc.compss.nio.dataRequest;

import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.nio.commands.Data;


public abstract class DataRequest {

    private final DataType type;
    private final Data source;
    private final String target;


    public DataRequest(DataType type, Data source, String target) {
        this.source = source;
        this.target = target;
        this.type = type;
    }

    public Data getSource() {
        return source;
    }

    public String getTarget() {
        return target;
    }

    public DataType getType() {
        return type;
    }

}
