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
package es.bsc.compss.types.data.access;

import es.bsc.compss.types.Application;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.data.accessparams.DirectoryAccessParams;
import es.bsc.compss.types.data.location.DataLocation;
import es.bsc.compss.types.data.location.ProtocolType;
import es.bsc.compss.types.data.operation.DirectoryTransferable;
import es.bsc.compss.types.data.params.DirectoryData;


/**
 * Handling of an access from the main code to a directory.
 */
public class DirectoryMainAccess extends FileMainAccess<DirectoryData, DirectoryAccessParams> {

    /**
     * Creates a new DirectoryMainAccess instance with the given mode {@code mode} and for the given file location
     * {@code loc}.
     *
     * @param app Id of the application accessing the file.
     * @param dir operation performed.
     * @param loc File location.
     * @return new DirectoryMainAccess instance
     */
    public static final DirectoryMainAccess constructDMA(Application app, Direction dir, DataLocation loc) {
        DirectoryAccessParams dap = DirectoryAccessParams.constructDAP(app, dir, loc);
        return new DirectoryMainAccess(app, dap);
    }

    private DirectoryMainAccess(Application app, DirectoryAccessParams p) {
        super(app, p);
    }

    @Override
    public DataLocation getUnavailableValueResponse() {
        return this.createExpectedLocalLocation("null");
    }

    @Override
    protected DirectoryTransferable createExpectedTransferable(boolean preserveSource) {
        return new DirectoryTransferable(preserveSource);
    }

    @Override
    protected ProtocolType expectedProtocol() {
        return ProtocolType.DIR_URI;
    }

}
