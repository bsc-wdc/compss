package es.bsc.compss.types.execution;

import java.util.List;


public interface InvocationParamCollection<T extends InvocationParam> extends InvocationParam {

    public List<T> getCollectionParameters();

    public void addParameter(T param);

    public int getSize();

}
