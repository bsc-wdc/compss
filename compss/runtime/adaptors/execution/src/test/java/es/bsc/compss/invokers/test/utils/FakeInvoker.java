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
package es.bsc.compss.invokers.test.utils;

import es.bsc.compss.execution.types.InvocationResources;
import es.bsc.compss.invokers.Invoker;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.execution.ExecutionSandbox;
import es.bsc.compss.types.execution.Invocation;
import es.bsc.compss.types.execution.InvocationContext;
import es.bsc.compss.types.execution.InvocationParam;
import es.bsc.compss.types.execution.exceptions.JobExecutionException;

import java.util.Iterator;
import java.util.List;
import storage.StubItf;


public class FakeInvoker extends Invoker {

    private final InvokerListener listener;


    public FakeInvoker(InvocationContext context, Invocation invocation, ExecutionSandbox sandbox,
        InvocationResources assignedResources, InvokerListener listener) throws JobExecutionException {
        super(context, invocation, sandbox, assignedResources);
        this.listener = listener;
    }

    @Override
    public void invokeMethod() throws JobExecutionException {
        List<Object> result = listener.runningMethod(this.invocation);
        Iterator<Object> objects = result.iterator();
        for (InvocationParam param : this.invocation.getParams()) {
            checkSCOPersistence(param);
        }
        if (this.invocation.getTarget() != null) {
            checkSCOPersistence(this.invocation.getTarget());
        }
        for (InvocationParam param : this.invocation.getResults()) {
            Object o = objects.next();
            param.setValue(o);
            if (o != null) {
                param.setValueClass(o.getClass());
            }
            checkSCOPersistence(param);
        }
        listener.methodReturn(result);
    }

    private void checkSCOPersistence(InvocationParam np) {
        boolean potentialPSCO = (np.getType().equals(DataType.OBJECT_T)) || (np.getType().equals(DataType.PSCO_T));
        if (np.isWriteFinalValue() && potentialPSCO) {
            Object obj = np.getValue();

            // Check if it is a PSCO and has been persisted in task
            String id = null;
            try {
                StubItf psco = (StubItf) obj;
                id = psco.getID();
            } catch (Exception e) {
                // No need to raise an exception because normal objects are not PSCOs
                id = null;
            }

            // Update to PSCO if needed
            if (id != null) {
                // Object has been persisted, we store the PSCO and change the value to its ID
                np.setType(DataType.PSCO_T);
            }
        }
    }


    public static interface InvokerListener {

        public List<Object> runningMethod(Invocation inv);

        public void methodReturn(List<Object> result);

    }


    @Override
    protected void cancelMethod() {
        // Nothing to do
    }
}
