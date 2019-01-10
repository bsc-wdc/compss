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
package es.bsc.compss.types.implementations;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import es.bsc.compss.types.resources.MethodResourceDescription;


public class MultiNodeImplementation extends AbstractMethodImplementation implements Externalizable {

    public static final int NUM_PARAMS = 2;

    private String declaringClass;
    private String methodName;


    public MultiNodeImplementation() {
        // For externalizable
        super();
    }

    public MultiNodeImplementation(String methodClass, String methodName, Integer coreId, Integer implementationId,
            MethodResourceDescription requirements) {

        super(coreId, implementationId, requirements);
        this.declaringClass = methodClass;
        this.methodName = methodName;
    }

    public String getDeclaringClass() {
        return declaringClass;
    }

    public void setDeclaringClass(String declaringClass) {
        this.declaringClass = declaringClass;
    }

    public String getMethodName() {
        return this.methodName;
    }

    public void setMethodName(String methodName) {
        this.methodName = methodName;
    }

    @Override
    public MethodType getMethodType() {
        return MethodType.MULTI_NODE;
    }

    @Override
    public String getMethodDefinition() {
        StringBuilder sb = new StringBuilder();
        sb.append("[DECLARING CLASS=").append(this.declaringClass);
        sb.append(", METHOD NAME=").append(this.methodName);
        sb.append("]");

        return sb.toString();
    }

    @Override
    public String toString() {
        return super.toString() + " Multi-Node Method declared in class " + this.declaringClass + "." + methodName + ": "
                + this.requirements.toString();
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        this.declaringClass = (String) in.readObject();
        this.methodName = (String) in.readObject();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeObject(this.declaringClass);
        out.writeObject(this.methodName);
    }

}
