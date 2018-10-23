/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package es.bsc.compss.local;

import es.bsc.compss.comm.Comm;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.annotations.parameter.Stream;
import es.bsc.compss.types.data.DataAccessId;
import es.bsc.compss.types.data.DataAccessId.RAccessId;
import es.bsc.compss.types.data.DataAccessId.RWAccessId;
import es.bsc.compss.types.data.DataAccessId.WAccessId;
import es.bsc.compss.types.execution.InvocationParam;
import es.bsc.compss.types.execution.InvocationParamURI;
import es.bsc.compss.types.parameter.BasicTypeParameter;
import es.bsc.compss.types.parameter.DependencyParameter;
import es.bsc.compss.types.parameter.Parameter;
import java.util.List;


/**
 *
 * @author flordan
 */
public class LocalParameter implements InvocationParam {

    private final Parameter param;

    private final boolean preserveSourceData;
    private final boolean writeFinalValue;

    private final String dataMgmtId;
    private String originalName;

    private Object value;
    private Class<?> valueClass;

    public LocalParameter(Parameter param) {
        this.param = param;
        DataType type = param.getType();
        switch (type) {
            case FILE_T:
            case OBJECT_T:
            case PSCO_T:
            case EXTERNAL_PSCO_T:
            case BINDING_OBJECT_T:
                DependencyParameter dPar = (DependencyParameter) param;
                DataAccessId dAccId = dPar.getDataAccessId();
                this.value = dPar.getDataTarget();
                boolean preserveSourceData = true;
                if (dAccId instanceof RAccessId) {
                    // Parameter is a R, has sources
                    preserveSourceData = ((RAccessId) dAccId).isPreserveSourceData();
                } else if (dAccId instanceof RWAccessId) {
                    // Parameter is a RW, has sources
                    preserveSourceData = ((RWAccessId) dAccId).isPreserveSourceData();
                } else {
                    // Parameter is a W, it has no sources
                    preserveSourceData = false;
                }

                // Check if the parameter has a valid PSCO and change its type
                // OUT objects are restricted by the API
                String renaming = null;
                DataAccessId faId = dPar.getDataAccessId();
                if (faId instanceof RWAccessId) {
                    // Read write mode
                    RWAccessId rwaId = (RWAccessId) faId;
                    renaming = rwaId.getReadDataInstance().getRenaming();
                    dataMgmtId = rwaId.getWrittenDataInstance().getRenaming();
                } else if (faId instanceof RAccessId) {
                    // Read only mode
                    RAccessId raId = (RAccessId) faId;
                    renaming = raId.getReadDataInstance().getRenaming();
                    dataMgmtId = renaming;
                } else {
                    WAccessId waId = (WAccessId) faId;
                    dataMgmtId = waId.getWrittenDataInstance().getRenaming();
                }
                if (renaming != null) {
                    String pscoId = Comm.getData(renaming).getPscoId();
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

                this.preserveSourceData = preserveSourceData;
                this.writeFinalValue = !(dAccId instanceof RAccessId); // Only store W and RW
                this.originalName = dPar.getOriginalName();
                break;
            default:
                //BASIC PARAMETERS
                BasicTypeParameter btParB = (BasicTypeParameter) param;
                this.value = btParB.getValue();
                this.preserveSourceData = false;
                this.writeFinalValue = false;
                this.dataMgmtId = null;
        }
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
    public Stream getStream() {
        return this.param.getStream();
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
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public List<? extends InvocationParamURI> getSources() {
        //Shouldn't be used on the local node
        return null;
    }

    public Parameter getParam() {
        return this.param;
    }

    @Override
    public String toString() {
        return this.getType() + " " + this.getValue() + " " + (this.isPreserveSourceData() ? "PRESERVE " : "VOLATILE ") + (this.isWriteFinalValue() ? "WRITE" : "DISMISS");
    }
}
