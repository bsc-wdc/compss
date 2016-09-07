package integratedtoolkit.types.request.ap;

import integratedtoolkit.components.impl.AccessProcessor;
import integratedtoolkit.components.impl.DataInfoProvider;
import integratedtoolkit.components.impl.TaskAnalyser;
import integratedtoolkit.components.impl.TaskDispatcher;


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
    public void process(AccessProcessor ap, TaskAnalyser ta, DataInfoProvider dip, TaskDispatcher<?, ?> td) {
        dip.setObjectVersionValue(renaming, value);
    }

    @Override
    public APRequestType getRequestType() {
        return APRequestType.SET_OBJECT_VERSION_VALUE;
    }

}
