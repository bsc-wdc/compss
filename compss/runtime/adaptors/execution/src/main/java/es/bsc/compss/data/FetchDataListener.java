package es.bsc.compss.data;

public interface FetchDataListener {

    /**
     * Notification that the value has been fetched properly.
     *
     * @param fetchedDataId Id of the data fetched.
     */
    public void fetchedValue(String fetchedDataId);

    /**
     * Notification of an error while fetching {@code failedDataId}.
     *
     * @param failedDataId Id of the data that could not be fetched.
     * @param cause reason why the value fetching failed.
     */
    public void errorFetchingValue(String failedDataId, Exception cause);

}
