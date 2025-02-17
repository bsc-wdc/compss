/*
 *  Copyright 2002-2025 Barcelona Supercomputing Center (www.bsc.es)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package es.bsc.compss.types.data.operation;

import es.bsc.compss.exceptions.FileDeletionException;
import es.bsc.compss.types.data.listener.EventListener;

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

    /**
     * Perform deletion.
     */
    public void perform() {
        LOGGER.debug("THREAD " + Thread.currentThread().getName() + " Delete " + this.getFile());
        try {
            if (!this.getFile().delete()) {
                FileDeletionException fde = new FileDeletionException("Error performing delete file");
                this.end(OperationEndState.OP_FAILED, fde);
                return;
            }
        } catch (SecurityException e) {
            this.end(OperationEndState.OP_FAILED, e);
            return;
        }
        this.end(OperationEndState.OP_OK);
    }

}
