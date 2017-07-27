package es.bsc.compss.types.data.operation;

import es.bsc.compss.types.data.listener.EventListener;
import es.bsc.compss.types.data.operation.DataOperation;

import java.io.File;


public class Delete extends DataOperation {

    protected File file;


    public Delete(File file, EventListener listener) {
        super(null, listener);
        this.file = file;
    }

    public File getFile() {
        return file;
    }

}
