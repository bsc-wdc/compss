/*
 *  Copyright 2002-2022 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.nio;

import es.bsc.compss.types.execution.InvocationParamCollection;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.LinkedList;
import java.util.List;


/**
 * Extension of the NIOParam class to handle collection types. Basically, a NIOParam plus a list of NIOParams
 * representing the contents of the collection.
 *
 * @see NIOParam
 */
public class NIOParamCollection extends NIOParam implements InvocationParamCollection<NIOParam> {

    private List<NIOParam> collectionParameters;


    /**
     * Create a new NIOParamCollection instance for externalization.
     */
    public NIOParamCollection() {
        // Only executed by externalizable
        super();
    }

    /**
     * Create a new NIOParamCollection copying the given NIOParam values.
     *
     * @param p NIOParam to copy.
     */
    public NIOParamCollection(NIOParam p) {
        super(p);

        // Empty attributes
        this.collectionParameters = new LinkedList<>();
    }

    /**
     * Returns the number of internal parameters of the collection.
     *
     * @return The number of internal parameters of the collection.
     */
    public int getSize() {
        return this.collectionParameters.size();
    }

    /**
     * Returns a list of objects containing the collection parameters.
     *
     * @return A list of objects containing the collection parameters.
     */
    public List<NIOParam> getCollectionParameters() {
        return this.collectionParameters;
    }

    /**
     * Adds a new parameter to the collection.
     *
     * @param p Parameter to add.
     */
    public void addParameter(NIOParam p) {
        this.collectionParameters.add(p);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);

        this.collectionParameters = new LinkedList<>();
        int numParameters = in.readInt();
        for (int i = 0; i < numParameters; ++i) {
            this.collectionParameters.add((NIOParam) in.readObject());
        }
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);

        out.writeInt(this.collectionParameters.size());
        for (NIOParam subParam : this.collectionParameters) {
            // Note that this implementation also implicitly supports nesting
            out.writeObject(subParam);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("[COLL_PARAM");
        dumpInternalInfo(sb);
        sb.append("]");

        return sb.toString();
    }

    /**
     * Creates a NIOResult instance describing how the task modifies the parameter value.
     * 
     * @return description of how the task modifies the parameter value.
     */
    @Override
    public NIOResult getResult() {
        List<NIOResult> elements = new LinkedList<>();

        for (NIOParam element : collectionParameters) {
            elements.add(element.getResult());
        }
        return new NIOResultCollection(getType(), this.getTargetPath(), elements);
    }
}
