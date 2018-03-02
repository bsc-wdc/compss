package streams.components.utils;

import java.util.UUID;


public class RegistrationId {

    private final String subscriberId;
    private final String consumerId;
    private final String producerId;


    public RegistrationId() {
        this.subscriberId = UUID.randomUUID().toString();
        this.consumerId = UUID.randomUUID().toString();
        this.producerId = UUID.randomUUID().toString();
    }

    public RegistrationId(String subscriberId, String consumerId, String producerId) {
        this.subscriberId = subscriberId;
        this.consumerId = consumerId;
        this.producerId = producerId;
    }

    public String getSubscriberId() {
        return this.subscriberId;
    }

    public String getConsumerId() {
        return this.consumerId;
    }

    public String getProducerId() {
        return this.producerId;
    }

}