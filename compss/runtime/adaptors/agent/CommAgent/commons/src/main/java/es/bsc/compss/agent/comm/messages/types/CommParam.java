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
package es.bsc.compss.agent.comm.messages.types;

import es.bsc.compss.agent.types.ApplicationParameter;
import es.bsc.compss.agent.types.RemoteDataInformation;
import es.bsc.compss.nio.NIOParam;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.parameter.StdIOStream;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;


/**
 * Class containing the parameter to invoke a main app/task on a Comm Agent.
 */
public class CommParam extends NIOParam implements ApplicationParameter, Externalizable {

    private Direction direction = Direction.IN;

    private RemoteDataInformation remoteData = null;


    /**
     * Constructs an empty CommParam.
     */
    public CommParam() {
        super();
    }

    /**
     * Constructs an initializes a CommParam.
     *
     * @param dataMgmtId id associated to the value
     * @param type type of the parameter value
     * @param direction direction of the parameter
     * @param stream stream to redirect to the parameter
     * @param prefix prefix to add to the parameter
     * @param name name of the parameter
     * @param originalName original name of the parameter value
     */
    public CommParam(String dataMgmtId, DataType type, Direction direction, StdIOStream stream, String prefix,
        String name, String contentType, double weight, boolean keepRename, String originalName) {
        super(dataMgmtId, type, stream, prefix, name, contentType, weight, keepRename, false, false, null, null,
            originalName);
        this.direction = direction;
    }

    /**
     * Creates a new CommParam instance copying the given CommParam internal fields.
     *
     * @param p CommParam to copy.
     */
    public CommParam(CommParam p) {
        super(p);
        this.direction = p.direction;
        this.remoteData = p.remoteData;
    }

    public void setDirection(Direction direction) {
        this.direction = direction;
    }

    @Override
    public Direction getDirection() {
        return direction;
    }

    @Override
    public String getParamName() {
        return super.getName();
    }

    @Override
    public String getContentType() {
        return super.getContentType();
    }

    public void setRemoteData(RemoteDataInformation remoteData) {
        this.remoteData = remoteData;
    }

    @Override
    public RemoteDataInformation getRemoteData() {
        return remoteData;
    }

    @Override
    public Object getValueContent() throws Exception {
        return super.getValue();
    }

    @Override
    public void writeExternal(ObjectOutput oo) throws IOException {
        super.writeExternal(oo);
        oo.writeInt(direction.ordinal());
        oo.writeObject(remoteData);
    }

    @Override
    public void readExternal(ObjectInput oi) throws IOException, ClassNotFoundException {
        super.readExternal(oi);
        direction = Direction.values()[oi.readInt()];
        remoteData = (RemoteDataInformation) oi.readObject();
    }

    /**
     * Dumps the internal information into the given StringBuilder.
     *
     * @param sb StringBuilder where to dump the internal information.
     */
    protected void dumpInternalInfo(StringBuilder sb) {
        sb.append("\"remoteData\":" + (remoteData == null ? "null" : remoteData) + ",");
        super.dumpInternalInfo(sb);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("{");
        dumpInternalInfo(sb);
        sb.append("}");
        return sb.toString();
    }

}
