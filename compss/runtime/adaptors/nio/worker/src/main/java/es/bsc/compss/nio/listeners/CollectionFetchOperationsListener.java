package es.bsc.compss.nio.listeners;

import es.bsc.compss.data.FetchDataListener;
import es.bsc.compss.data.MultiOperationFetchListener;


public class CollectionFetchOperationsListener extends MultiOperationFetchListener {

    private final String collectionDataId;
    private final FetchDataListener listener;


    public CollectionFetchOperationsListener(String collectionDataId, FetchDataListener listener) {
        this.collectionDataId = collectionDataId;
        this.listener = listener;
    }

    @Override
    public void doCompleted() {
        this.listener.fetchedValue(this.collectionDataId);
    }

    @Override
    public void doFailure(String failedDataId, Exception e) {
        this.listener.errorFetchingValue(this.collectionDataId, e);
    }
}
