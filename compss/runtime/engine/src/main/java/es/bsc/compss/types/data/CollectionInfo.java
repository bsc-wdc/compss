package es.bsc.compss.types.data;

/**
 * Information about a collection and its versions
 * @see DataInfo
 * @see es.bsc.compss.components.impl.DataInfoProvider registerCollectionAccess method
 */
public class CollectionInfo extends DataInfo {

    private String collectionId;

    /**
     * Default constructor
     * @see DataInfo empty constructor
     */
    public CollectionInfo() {
        super();
    }

    /**
     * Constructor with collection identifier
     * @param collectionId Collection identifier
     */
    public CollectionInfo(String collectionId) {
        super();
        this.collectionId = collectionId;
    }

    /**
     * Get the collectionId
     * @return String
     */
    public String getCollectionId() {
        return collectionId;
    }

    /**
     * Change the value of the collectionId
     * @param collectionId String
     */
    public void setCollectionId(String collectionId) {
        this.collectionId = collectionId;
    }
}
