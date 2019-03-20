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
package es.bsc.compss.nio;

import es.bsc.compss.nio.commands.NIOData;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.annotations.parameter.Stream;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.LinkedList;
import java.util.List;


/**
 * Extension of the NIOParam class to handle collection types.
 * 
 * @see NIOParam
 */
public class NIOParamCollection extends NIOParam {

    // A NIOParamCollection is basically a NIOParam plus a list of NIOParams (the contents of the collection)
    private List<NIOParam> collectionParameters = new LinkedList<>();


    /**
     * Constructor. Same as NIOParam
     * 
     * @param dataMgmtId String
     * @param type DataType
     * @param stream Stream
     * @param prefix String
     * @param name String
     * @param preserveSourceData Boolean
     * @param writeFinalValue Boolean
     * @param value Object
     * @param data NIOData
     * @param originalName String
     * @see NIOParam Constructor
     */
    public NIOParamCollection(String dataMgmtId, DataType type, Stream stream, String prefix, String name,
            boolean preserveSourceData, boolean writeFinalValue, Object value, NIOData data, String originalName) {
        super(dataMgmtId, type, stream, prefix, name, preserveSourceData, writeFinalValue, value, data, originalName);
    }

    public NIOParamCollection() {

    }

    /**
     * Getter of the parameter list. Use this method to insert elements in there
     * 
     * @return List
     */
    public List<NIOParam> getCollectionParameters() {
        return collectionParameters;
    }

    /**
     * Extend the readExternal from NIOParam. The implementation of this method speed up the serialization and
     * deserialization processes
     * 
     * @param in Object to read
     * @throws IOException
     * @throws ClassNotFoundException
     */
    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        this.collectionParameters = new LinkedList<>();
        int numParameters = in.readInt();
        while (numParameters-- > 0) {
            collectionParameters.add((NIOParam) in.readObject());
        }
    }

    /**
     * Extend the writeExternal from NIOParam. The implementation of this method speed up the serialization and
     * deserialization processes.
     * 
     * @param out Object in which we write the representation of the NIOParamCollection
     * @throws IOException
     */
    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeInt(collectionParameters.size());
        for (NIOParam subParam : collectionParameters) {
            // Note that this implementation also implicitly supports nesting
            out.writeObject(subParam);
        }
    }
}
