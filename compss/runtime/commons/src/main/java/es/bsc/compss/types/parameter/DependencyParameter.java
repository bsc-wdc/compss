package es.bsc.compss.types.parameter;

import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.parameter.Stream;

import es.bsc.compss.types.data.DataAccessId;
import es.bsc.compss.types.data.Transferable;


public class DependencyParameter extends Parameter implements Transferable {

    /**
     * Serializable objects Version UID are 1L in all Runtime
     */
    private static final long serialVersionUID = 1L;

    public static final String NO_NAME = "NO_NAME";

    private DataAccessId daId;
    private Object dataSource;
    private String dataTarget; // Full path with PROTOCOL


    public DependencyParameter(DataType type, Direction direction, Stream stream, String prefix) {
        super(type, direction, stream, prefix);
    }

    public DataAccessId getDataAccessId() {
        return daId;
    }

    public void setDataAccessId(DataAccessId daId) {
        this.daId = daId;
    }

    @Override
    public Object getDataSource() {
        return dataSource;
    }

    @Override
    public void setDataSource(Object dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public String getDataTarget() {
        return this.dataTarget;
    }

    @Override
    public void setDataTarget(String target) {
        this.dataTarget = target;
    }

    public String getOriginalName() {
        return NO_NAME;
    }

    @Override
    public String toString() {
        return "DependencyParameter";
    }

}
