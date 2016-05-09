package integratedtoolkit.types.request.ap;

import integratedtoolkit.components.impl.AccessProcessor;
import integratedtoolkit.components.impl.DataInfoProvider;
import integratedtoolkit.components.impl.TaskAnalyser;
import integratedtoolkit.components.impl.TaskDispatcher;
import integratedtoolkit.types.data.FileInfo;
import integratedtoolkit.types.data.location.DataLocation;

public class DeleteFileRequest extends APRequest {

    private final DataLocation loc;

    public DeleteFileRequest(DataLocation loc) {
        this.loc = loc;
    }

    public DataLocation getLocation() {
        return loc;
    }

    @Override
    public void process(AccessProcessor ap, TaskAnalyser ta, DataInfoProvider dip, TaskDispatcher td) {
        FileInfo fileInfo = dip.deleteData(loc);

        if (fileInfo == null) { //file is not used by any task
            java.io.File f = new java.io.File(loc.getPath());
            f.delete();
        } else { // file is involved in some task execution
            // File Won't be read by any future task or from the main code. 
            // Remove it from the dependency analysis and the files to be transferred back
            ta.deleteFile(fileInfo);
        }
    }

    @Override
    public APRequestType getRequestType() {
        return APRequestType.DELETE_FILE;
    }

}
