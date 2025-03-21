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
package es.bsc.compss.types;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;


public class CollectionLayout implements Externalizable {

    private String paramName = "";
    private int blockCount;
    private int blockLen;
    private int blockStride;


    /**
     * Default Constructor.
     */
    public CollectionLayout() {
        this.paramName = "";
        this.blockCount = -1;
        this.blockLen = -1;
        this.blockStride = -1;
    }

    /**
     * Collection layout constructor.
     * 
     * @param paramName Name of the parameter.
     * @param blockCount Block count.
     * @param blockLen Block length.
     * @param blockStride Block Stride.
     */
    public CollectionLayout(String paramName, int blockCount, int blockLen, int blockStride) {
        this.paramName = paramName;
        this.blockCount = blockCount;
        this.blockLen = blockLen;
        this.blockStride = blockStride;
    }

    public String getParamName() {
        return this.paramName;
    }

    public int getBlockCount() {
        return this.blockCount;
    }

    public int getBlockLen() {
        return this.blockLen;
    }

    public int getBlockStride() {
        return this.blockStride;
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.paramName = (String) in.readObject();
        this.blockCount = in.readInt();
        this.blockLen = in.readInt();
        this.blockStride = in.readInt();

    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(this.paramName);
        out.writeInt(this.blockCount);
        out.writeInt(this.blockLen);
        out.writeInt(this.blockStride);

    }

    /**
     * Check if it is an empty collection layout.
     * 
     * @return True if is an empty layout. Otherwise false
     */
    public boolean isEmpty() {
        return (this.blockCount + this.blockLen + this.blockStride) < 0;
    }

    @Override
    public String toString() {
        return "Param_name: " + this.paramName + ", block_count: " + this.blockCount + ", block_len: " + this.blockLen
            + ", block_stride: " + this.blockStride;
    }

}
