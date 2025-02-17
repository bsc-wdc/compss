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
package es.bsc.compss.invokers.test.utils.types;

import static org.junit.Assert.fail;

import es.bsc.compss.types.execution.Invocation;
import es.bsc.compss.types.execution.InvocationParam;


public class InvocationParameterAction {

    public static enum Field {
        TYPE, MGMT_NAME, VALUE, VALUE_CLASS, PREFIX, STREAM, ORIGINAL_NAME, WRITE_FINAL
    }

    public static enum Action {
        CREATE, CREATE_PERSISTENT, UPDATE, PERSIST
    }


    private final Role role;
    private final int paramIdx;
    private final Object value;
    private final Action action;


    /**
     * Invocation Parameter Action constructor.
     * 
     * @param role Role
     * @param paramIdx Parameters Identifier
     * @param action Action
     * @param value Value
     */
    public InvocationParameterAction(Role role, int paramIdx, Action action, Object value) {
        this.role = role;
        this.paramIdx = paramIdx;
        this.action = action;
        this.value = value;
    }

    public Role getRole() {
        return role;
    }

    public Object getValue() {
        return value;
    }

    public Action getAction() {
        return action;
    }

    /**
     * Obtain invocation parameter.
     * 
     * @param inv Invocation
     * @return
     */
    public InvocationParam obtain(Invocation inv) {
        InvocationParam param;
        String caseId;
        switch (role) {
            case ARGUMENT:
                caseId = "argument " + paramIdx + " parameter";
                param = inv.getParams().get(paramIdx);
                break;
            case TARGET:
                caseId = "target parameter";
                param = inv.getTarget();
                break;
            default:
                caseId = "return parameter";
                param = inv.getResults().get(paramIdx);
                break;
        }

        if (param == null) {
            fail("Unexpectedly empty " + caseId + ".");
            return null;
        }
        return param;
    }

    @Override
    public String toString() {
        String caseId;
        switch (role) {
            case ARGUMENT:
                caseId = "argument " + paramIdx + " parameter";
                break;
            case TARGET:
                caseId = "target parameter";
                break;
            default:
                caseId = "return parameter";
                break;
        }
        return action + " on " + caseId;
    }

}
