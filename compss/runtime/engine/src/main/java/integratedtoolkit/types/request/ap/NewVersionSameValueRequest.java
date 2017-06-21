package integratedtoolkit.types.request.ap;

import integratedtoolkit.components.impl.AccessProcessor;
import integratedtoolkit.components.impl.DataInfoProvider;
import integratedtoolkit.components.impl.TaskAnalyser;
import integratedtoolkit.components.impl.TaskDispatcher;

public class NewVersionSameValueRequest extends APRequest {

    private String rRenaming;
    private String wRenaming;

    public NewVersionSameValueRequest(String rRenaming, String wRenaming) {
        super();
        this.rRenaming = rRenaming;
        this.wRenaming = wRenaming;
    }

    public String getrRenaming() {
        return rRenaming;
    }

    public void setrRenaming(String rRenaming) {
        this.rRenaming = rRenaming;
    }

    public String getwRenaming() {
        return wRenaming;
    }

    public void setwRenaming(String wRenaming) {
        this.wRenaming = wRenaming;
    }

    @Override
    public void process(AccessProcessor ap, TaskAnalyser ta, DataInfoProvider dip, TaskDispatcher td) {
        dip.newVersionSameValue(rRenaming, wRenaming);
    }

    @Override
    public APRequestType getRequestType() {
        return APRequestType.NEW_VERSION_SAME_VALUE;
    }

}
