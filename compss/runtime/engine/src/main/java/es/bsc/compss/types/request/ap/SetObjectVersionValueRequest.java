package es.bsc.compss.types.request.ap;

import es.bsc.compss.components.impl.AccessProcessor;
import es.bsc.compss.components.impl.DataInfoProvider;
import es.bsc.compss.components.impl.TaskAnalyser;
import es.bsc.compss.components.impl.TaskDispatcher;

public class SetObjectVersionValueRequest extends APRequest {

    private String renaming;
    private Object value;

    public SetObjectVersionValueRequest(String renaming, Object value) {
        this.renaming = renaming;
        this.value = value;
    }

    public String getRenaming() {
        return renaming;
    }

    public void setRenaming(String renaming) {
        this.renaming = renaming;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    @Override
    public void process(AccessProcessor ap, TaskAnalyser ta, DataInfoProvider dip, TaskDispatcher td) {
        dip.setObjectVersionValue(renaming, value);
    }

    @Override
    public APRequestType getRequestType() {
        return APRequestType.SET_OBJECT_VERSION_VALUE;
    }

}
