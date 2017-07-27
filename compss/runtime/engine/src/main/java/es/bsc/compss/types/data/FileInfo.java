package es.bsc.compss.types.data;

import es.bsc.compss.types.data.location.DataLocation;


public class FileInfo extends DataInfo {

    // Original name and location of the file
    private final DataLocation origLocation;


    public FileInfo(DataLocation loc) {
        super();
        this.origLocation = loc;
    }

    public DataLocation getOriginalLocation() {
        return origLocation;
    }

}
