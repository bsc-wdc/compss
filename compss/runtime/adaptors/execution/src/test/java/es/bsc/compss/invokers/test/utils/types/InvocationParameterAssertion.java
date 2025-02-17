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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import es.bsc.compss.invokers.test.utils.types.Role;
import es.bsc.compss.types.execution.Invocation;
import es.bsc.compss.types.execution.InvocationParam;


public class InvocationParameterAssertion {

    public static enum Field {
        TYPE, MGMT_NAME, VALUE, VALUE_CLASS, PREFIX, STREAM, ORIGINAL_NAME, WRITE_FINAL
    }


    private final Role role;
    private final int paramIdx;
    private final Field field;
    private final Object value;


    /**
     * Invocation Parameter Assertion constructor.
     * 
     * @param role Role
     * @param paramIdx Param Identifier
     * @param field Field
     * @param value Value
     */
    public InvocationParameterAssertion(Role role, int paramIdx, Field field, Object value) {
        this.role = role;
        this.paramIdx = paramIdx;
        this.field = field;
        this.value = value;
    }

    public Role getRole() {
        return role;
    }

    public Field getField() {
        return field;
    }

    public Object getValue() {
        return value;
    }

    /**
     * Validate.
     * 
     * @param inv Invocation
     */
    public void validate(Invocation inv) {
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
                caseId = "return " + paramIdx + " parameter";
                param = inv.getResults().get(paramIdx);
                break;
        }

        if (param == null) {
            fail("Unexpectedly empty " + caseId + ".");
            return;
        }
        switch (field) {
            case TYPE:
                assertEquals(
                    "Unexpected type for " + caseId + " (expected " + value + " and got " + param.getType() + ")",
                    param.getType(), value);
                break;
            case VALUE:
                assertEquals(
                    "Unexpected value for " + caseId + " (expected " + value + " and got " + param.getValue() + ")",
                    param.getValue(), value);
                break;
            case VALUE_CLASS:
                assertEquals("Unexpected class for " + caseId + " (expected " + value + " and got "
                    + param.getValueClass() + ")", param.getValueClass(), value);
                break;
            case PREFIX:
                assertEquals(
                    "Unexpected prefix for " + caseId + " (expected " + value + " and got " + param.getPrefix() + ")",
                    param.getPrefix(), value);
                break;
            case STREAM:
                assertEquals("Unexpected stream for " + caseId + " (expected " + value + " and got "
                    + param.getStdIOStream() + ")", param.getStdIOStream(), value);
                break;
            case ORIGINAL_NAME:
                assertEquals("Unexpected name for " + caseId + " (expected " + value + " and got "
                    + param.getOriginalName() + ")", param.getOriginalName(), value);
                break;
            case MGMT_NAME:
                assertEquals("Unexpected RENAME/PSCO ID for " + caseId + " (expected " + value + " and got "
                    + param.getDataMgmtId() + ")", param.getDataMgmtId(), value);
                break;
            default:// WRITE_FINAL
                assertEquals("Unexpected isWriteFinal for " + caseId + " (expected " + value + " and got "
                    + param.isWriteFinalValue() + ")", param.isWriteFinalValue(), value);
                break;
        }
    }

}
