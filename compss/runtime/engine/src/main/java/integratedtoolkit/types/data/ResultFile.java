package integratedtoolkit.types.data;

import integratedtoolkit.types.data.location.DataLocation;

import java.io.File;


public class ResultFile implements Comparable<ResultFile> {

    private DataInstanceId fId;
    private DataLocation originalLocation;


    public ResultFile(DataInstanceId fId, DataLocation location) {
        this.fId = fId;
        this.originalLocation = location;
    }

    public DataInstanceId getFileInstanceId() {
        return fId;
    }

    public DataLocation getOriginalLocation() {
        return originalLocation;
    }

    public String getOriginalName() {
        String[] splitPath = originalLocation.getPath().split(File.separator);
        return splitPath[splitPath.length - 1];
    }

    // Comparable interface implementation
    @Override
    public int compareTo(ResultFile resFile) throws NullPointerException {
        if (resFile == null) {
            throw new NullPointerException();
        }

        // Compare file identifiers
        return this.getFileInstanceId().compareTo(resFile.getFileInstanceId());
    }

    @Override
    public String toString() {
        return fId.getRenaming();
    }

}
