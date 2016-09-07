package integratedtoolkit.types.data;

import integratedtoolkit.api.COMPSsRuntime.DataType;


public interface Transferable {

    /**
     * Returns the source data
     * 
     * @return
     */
    public Object getDataSource();

    /**
     * Sets the source data
     * 
     * @param dataSource
     */
    public void setDataSource(Object dataSource);

    /**
     * Returns the target data
     * 
     * @return
     */
    public String getDataTarget();

    /**
     * Sets the target data
     * 
     * @param target
     */
    public void setDataTarget(String target);

    /**
     * Returns the data Transfer type
     * 
     * @return
     */
    public DataType getType();

}
