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

import es.bsc.compss.types.execution.InvocationParamDictCollection;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.Map;


/**
 * Extension of the NIOParam class to handle dictionary collection types. Basically, a NIOParam plus a map of NIOParams
 * representing the contents of the dictionary collection.
 * 
 * @see NIOParam
 */
public class NIOParamDictCollection extends NIOParam implements InvocationParamDictCollection<NIOParam, NIOParam> {

    private Map<NIOParam, NIOParam> dictCollectionParameters;


    /**
     * Create a new NIOParamCollection instance for externalization.
     */
    public NIOParamDictCollection() {
        // Only executed by externalizable
        super();
    }

    /**
     * Create a new NIOParamDictCollection copying the given NIOParam values.
     * 
     * @param p NIOParam to copy.
     */
    public NIOParamDictCollection(NIOParam p) {
        super(p);

        // Empty attributes
        this.dictCollectionParameters = new HashMap<>();
    }

    /**
     * Returns the number of internal parameters of the dictionary collection.
     * 
     * @return The number of internal parameters of the dictionary collection.
     */
    public int getSize() {
        return this.dictCollectionParameters.size();
    }

    /**
     * Returns a list of objects containing the dictionary collection parameters.
     * 
     * @return A list of objects containing the dictionary collection parameters.
     */
    public Map<NIOParam, NIOParam> getDictCollectionParameters() {
        return this.dictCollectionParameters;
    }

    /**
     * Adds a new parameter to the dictionary collection.
     * 
     * @param k Key parameter to add.
     * @param v Value parameter to add.
     */
    public void addParameter(NIOParam k, NIOParam v) {
        this.dictCollectionParameters.put(k, v);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);

        this.dictCollectionParameters = new HashMap<>();
        int numParameters = in.readInt();
        for (int i = 0; i < numParameters; ++i) {
            this.dictCollectionParameters.put((NIOParam) in.readObject(), (NIOParam) in.readObject());
        }
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);

        out.writeInt(this.dictCollectionParameters.size());
        for (Map.Entry<NIOParam, NIOParam> entry : this.dictCollectionParameters.entrySet()) {
            // Note that this implementation also implicitly supports nesting
            out.writeObject(entry.getKey());
            out.writeObject(entry.getValue());
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("[DICT_COLL_PARAM");
        dumpInternalInfo(sb);
        sb.append("]");

        return sb.toString();
    }
}
