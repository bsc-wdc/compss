/*         
 *  Copyright 2002-2018 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.invokers.test.utils;

import es.bsc.compss.COMPSsConstants.Lang;
import es.bsc.compss.COMPSsConstants.TaskExecution;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.execution.Invocation;
import es.bsc.compss.types.execution.InvocationContext;
import es.bsc.compss.types.execution.InvocationParam;
import es.bsc.compss.types.execution.LanguageParams;
import java.io.PrintStream;
import storage.StorageException;
import storage.StubItf;


public class FakeInvocationContext implements InvocationContext {

    private final String hostName;
    private final String appDir;
    private final String installDir;
    private final String libPath;
    private final String workingDir;
    private final PrintStream out;
    private final PrintStream err;
    private final InvocationContextListener listener;


    private FakeInvocationContext() {
        hostName = "localhost";
        appDir = "";
        installDir = "";
        libPath = "";
        workingDir = "";
        out = System.out;
        err = System.err;
        listener = null;
    }

    private FakeInvocationContext(String hostName, String appDir, String installDir, String libPath, String wDir, PrintStream out,
            PrintStream err, InvocationContextListener listener) {
        this.hostName = hostName;
        this.appDir = appDir;
        this.installDir = installDir;
        this.libPath = libPath;
        this.workingDir = wDir;
        this.out = out;
        this.err = err;
        this.listener = listener;
    }

    @Override
    public String getHostName() {
        return this.hostName;
    }

    @Override
    public String getAppDir() {
        return this.appDir;
    }

    @Override
    public String getInstallDir() {
        return this.installDir;
    }

    @Override
    public String getLibPath() {
        return this.libPath;
    }

    @Override
    public String getWorkingDir() {
        return this.workingDir;
    }

    @Override
    public TaskExecution getExecutionType() {
        return TaskExecution.COMPSS;
    }

    @Override
    public boolean isPersistentEnabled() {
        return false;
    }

    @Override
    public LanguageParams getLanguageParams(Lang lang) {
        return new LanguageParams() {
        };
    }

    @Override
    public void registerOutputs(String outputsBasename) {
        // Do nothing
    }

    @Override
    public void unregisterOutputs() {
        // Do nothing
    }

    @Override
    public String getStandardStreamsPath(Invocation invocation) {
        return null;
    }

    @Override
    public PrintStream getThreadOutStream() {
        return this.out;
    }

    @Override
    public PrintStream getThreadErrStream() {
        return this.err;
    }

    @Override
    public String getStorageConf() {
        return null;
    }

    @Override
    public void loadParam(InvocationParam param) throws StorageException {
        Object o;
        switch (param.getType()) {
            case OBJECT_T:
                o = getObject(param.getDataMgmtId());
                if (o != null) {
                    param.setValue(o);
                    break;
                }
            case PSCO_T:
                o = getPersistentObject(param.getDataMgmtId());
                if (o != null) {
                    param.setType(DataType.PSCO_T);
                    param.setValue(o);
                }
                break;
            default:
        }
    }

    private Object getObject(String rename) {
        if (this.listener != null) {
            return listener.getObject(rename);
        }
        return null;
    }

    private Object getPersistentObject(String id) throws StorageException {
        if (this.listener != null) {
            return listener.getPersistentObject(id);
        }
        return null;
    }

    @Override
    public void storeParam(InvocationParam param) {
        switch (param.getType()) {
            case OBJECT_T:
                storeObject(param.getDataMgmtId(), param.getValue());
                break;
            case PSCO_T:
                String dataId = ((StubItf) param.getValue()).getID();
                storePersistentObject(dataId, param.getValue());
                break;
            default:
                // do nothing
        }
    }

    public void storeObject(String renaming, Object value) {
        if (this.listener != null) {
            listener.storeObject(renaming, value);
        }
    }

    public void storePersistentObject(String id, Object obj) {
        if (this.listener != null) {
            listener.storePersistentObject(id, obj);
        }
    }

    @Override
    public long getTracingHostID() {
        return 0;
    }


    public static class Builder {

        final FakeInvocationContext context;


        public Builder() {
            context = new FakeInvocationContext();
        }

        public Builder(FakeInvocationContext context) {
            this.context = context;
        }

        public Builder setListener(InvocationContextListener listener) {
            return new Builder(new FakeInvocationContext(context.hostName, context.appDir, context.installDir, context.libPath,
                    context.workingDir, context.out, context.err, listener));
        }

        public FakeInvocationContext build() {
            return context;
        }
    }

    public static interface InvocationContextListener {

        public Object getObject(String rename);

        public Object getPersistentObject(String id);

        public void storePersistentObject(String id, Object obj);

        public void storeObject(String renaming, Object value);

    }

}
