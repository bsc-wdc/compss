package integratedtoolkit.types.data;

import integratedtoolkit.api.COMPSsRuntime.DataType;


public interface Transferable {

    public Object getDataSource();

    public void setDataSource(Object dataSource);

    public String getDataTarget();

    public void setDataTarget(String target);
    
    public DataType getType();
}
