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
import java.util.Iterator;
import java.util.Map;


/**
 * Extension of the NIOParam class to handle dictionary collection types. Basically, a NIOParam plus a map of NIOParams
 * representing the contents of the dictionary collection.
 * 
 * @see NIOParam
 */
public class NIOParamDictCollection extends NIOParamCollection implements InvocationParamDictCollection<NIOParam> {

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
    }

    @Override
    public int getSize() {
        return super.getSize() / 2;
    }

    @Override
    public Map<NIOParam, NIOParam> getDictionary() {
        Map<NIOParam, NIOParam> map = new HashMap<>();
        Iterator<NIOParam> elements = super.getCollectionParameters().iterator();
        while (elements.hasNext()) {
            NIOParam k = elements.next();
            NIOParam v = elements.next();
            map.put(k, v);
        }
        return map;
    }

    @Override
    public void addEntry(NIOParam k, NIOParam v) {
        super.addElement(k);
        super.addElement(v);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);

    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("[DICT_COLL_PARAM");
        dumpInternalInfo(sb);
        sb.append("]");

        return sb.toString();
    }
}
