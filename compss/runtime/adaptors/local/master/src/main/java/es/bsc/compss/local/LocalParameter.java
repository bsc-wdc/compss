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
package es.bsc.compss.local;

import es.bsc.compss.comm.Comm;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.annotations.parameter.StdIOStream;
import es.bsc.compss.types.data.DataAccessId;
import es.bsc.compss.types.data.DataAccessId.ReadingDataAccessId;
import es.bsc.compss.types.data.DataAccessId.WritingDataAccessId;
import es.bsc.compss.types.execution.InvocationParam;
import es.bsc.compss.types.execution.InvocationParamURI;
import es.bsc.compss.types.parameter.BasicTypeParameter;
import es.bsc.compss.types.parameter.DependencyParameter;
import es.bsc.compss.types.parameter.Parameter;

import java.util.List;


public class LocalParameter implements InvocationParam {

    private final Parameter param;
    private final DataType originalType;

    private final boolean preserveSourceData;
    private final boolean writeFinalValue;

    private final String dataMgmtId;
    private final String sourceDataMgmtId;
    private String originalName;
    private String renamedName;

    private Object value;
    private Class<?> valueClass;
    private boolean forwardedResult;


    /**
     * Creates a new LocalParameter instance for externalization.
     */
    public LocalParameter() {
        // Only executed by externalizable
        this.param = null;
        this.originalType = null;
        this.preserveSourceData = false;
        this.writeFinalValue = false;
        this.sourceDataMgmtId = null;
        this.dataMgmtId = null;
        this.forwardedResult = false;
    }

    /**
     * Creates a new LocalParameter instance from the given parameter information.
     * 
     * @param param Parameter information.
     */
    public LocalParameter(Parameter param) {
        this.param = param;
        DataType type = param.getType();
        this.originalType = type;
        switch (type) {
            case FILE_T:
            case DIRECTORY_T:
            case OBJECT_T:
            case COLLECTION_T:
            case DICT_COLLECTION_T:
            case STREAM_T:
            case PSCO_T:
            case EXTERNAL_STREAM_T:
            case EXTERNAL_PSCO_T:
            case BINDING_OBJECT_T:
                DependencyParameter dPar = (DependencyParameter) param;
                this.value = dPar.getDataTarget();
                this.preserveSourceData = dPar.isSourcePreserved();

                // Check if the parameter has a valid PSCO and change its type
                // OUT objects are restricted by the API
                DataAccessId faId = dPar.getDataAccessId();
                if (faId.isWrite()) {
                    this.dataMgmtId = ((WritingDataAccessId) faId).getWrittenDataInstance().getRenaming();
                    if (faId.isRead()) {
                        this.sourceDataMgmtId = "tmp" + this.dataMgmtId;
                    } else {
                        this.sourceDataMgmtId = null;
                    }
                } else {
                    this.sourceDataMgmtId = ((ReadingDataAccessId) faId).getReadDataInstance().getRenaming();
                    this.dataMgmtId = this.sourceDataMgmtId;
                }
                if (this.sourceDataMgmtId != null) {
                    String pscoId = Comm.getData(this.sourceDataMgmtId).getPscoId();
                    if (pscoId != null) {
                        if (type.equals(DataType.OBJECT_T)) {
                            // Change Object type if it is a PSCO
                            param.setType(DataType.PSCO_T);
                        } else if (type.equals(DataType.FILE_T)) {
                            // Change external object type (Workaround for Python PSCO return objects)
                            param.setType(DataType.EXTERNAL_PSCO_T);
                        }
                        this.param.setType(param.getType());
                    }
                }
                this.writeFinalValue = faId.isWrite();
                this.originalName = dPar.getOriginalName();
                break;
            default:
                // BASIC PARAMETERS
                BasicTypeParameter btParB = (BasicTypeParameter) param;
                this.value = btParB.getValue();
                this.preserveSourceData = false;
                this.writeFinalValue = false;
                this.sourceDataMgmtId = null;
                this.dataMgmtId = null;
        }
        this.forwardedResult = false;
    }

    @Override
    public String getName() {
        return this.param.getName();
    }

    @Override
    public String getContentType() {
        return this.param.getContentType();
    }

    @Override
    public void setType(DataType type) {
        this.param.setType(type);
    }

    @Override
    public DataType getType() {
        return this.param.getType();
    }

    @Override
    public boolean isCollective() {
        return false;
    }

    /**
     * Returns the original type for the parameter.
     * 
     * @return Original type of the parameter.
     */
    public DataType getOriginalType() {
        return originalType;
    }

    @Override
    public boolean isPreserveSourceData() {
        return this.preserveSourceData;
    }

    @Override
    public boolean isWriteFinalValue() {
        return this.writeFinalValue;
    }

    @Override
    public String getPrefix() {
        return this.param.getPrefix();
    }

    @Override
    public StdIOStream getStdIOStream() {
        return this.param.getStream();
    }

    @Override
    public double getWeight() {
        return this.param.getWeight();
    }

    @Override
    public boolean isKeepRename() {
        return this.param.isKeepRename();
    }

    @Override
    public String getOriginalName() {
        return this.originalName;
    }

    @Override
    public void setOriginalName(String originalName) {
        this.originalName = originalName;
    }

    @Override
    public String getRenamedName() {
        return this.renamedName;
    }

    @Override
    public void setRenamedName(String renamedName) {
        this.renamedName = renamedName;
    }

    @Override
    public Object getValue() {
        return this.value;
    }

    @Override
    public void setValue(Object val) {
        this.value = val;
    }

    @Override
    public void setValueClass(Class<?> valClass) {
        this.valueClass = valClass;
    }

    @Override
    public Class<?> getValueClass() {
        return this.valueClass;
    }

    @Override
    public String getDataMgmtId() {
        return this.dataMgmtId;
    }

    @Override
    public String getSourceDataId() {
        return this.sourceDataMgmtId;
    }

    @Override
    public List<? extends InvocationParamURI> getSources() {
        // Shouldn't be used on the local node
        return null;
    }

    @Override
    public void resultIsForwarded() {
        this.forwardedResult = true;
    }

    @Override
    public boolean isForwardedResult() {
        return this.forwardedResult;
    }

    public Parameter getParam() {
        return this.param;
    }

    @Override
    public String toString() {
        return this.getType() + " " + this.getValue() + " " + (this.isPreserveSourceData() ? "PRESERVE" : "VOLATILE")
            + " " + (this.isWriteFinalValue() ? "WRITE" : "DISMISS") + " originalName " + originalName + " renamedName "
            + renamedName + " sourceData " + sourceDataMgmtId + " finalData " + dataMgmtId;
    }
}
