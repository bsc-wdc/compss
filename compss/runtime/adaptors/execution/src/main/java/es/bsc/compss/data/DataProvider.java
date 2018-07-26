package es.bsc.compss.data;

import es.bsc.compss.types.execution.InvocationParam;


/**
 *
 * @author flordan
 */
public interface DataProvider {

    public void askForTransfer(InvocationParam param, int index, DataManager.LoadDataListener tt);

}
