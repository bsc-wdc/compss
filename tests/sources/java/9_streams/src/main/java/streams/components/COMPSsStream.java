package streams.components;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import streams.components.exceptions.AnnounceException;
import streams.components.exceptions.InvalidCredentialsException;
import streams.components.exceptions.SubscribeException;
import streams.components.utils.RegistrationId;


public class COMPSsStream {

    private static final String PROPERTIES_FILE_PATH_CONSUMER = "/home/cramonco/svn/compss/framework/trunk/tests/sources/basic/77-streams/src/main/resources/consumer.props";
    private static final String PROPERTIES_FILE_PATH_PRODUCER = "/home/cramonco/svn/compss/framework/trunk/tests/sources/basic/77-streams/src/main/resources/producer.props";

    private final Map<String, List<String>> subscriberIds2consumerIds;
    private final Map<String, String> consumerIds2subscriberIds;
    private final Map<String, KafkaConsumer<String, String>> consumerIds2consumers;
    private final Map<String, List<String>> consumerIds2topics;

    private final Map<String, List<String>> subscriberIds2producerIds;
    private final Map<String, String> producerIds2subscriberIds;
    private final Map<String, KafkaProducer<String, String>> producerIds2producers;


    /**
     * Initialize a new stream instance
     * 
     */
    public COMPSsStream() {
        this.subscriberIds2consumerIds = new HashMap<>();
        this.consumerIds2subscriberIds = new HashMap<>();
        this.consumerIds2consumers = new HashMap<>();
        this.consumerIds2topics = new HashMap<>();

        this.subscriberIds2producerIds = new HashMap<>();
        this.producerIds2subscriberIds = new HashMap<>();
        this.producerIds2producers = new HashMap<>();
    }

    /*
     * ****************************************************************************************************************
     * CONSUMER METHODS
     * ****************************************************************************************************************
     */

    public RegistrationId subscribe(List<String> topics) throws SubscribeException {
        // Create subscription id
        RegistrationId registrationId = new RegistrationId();

        // Create internal consumer
        Properties properties = new Properties();
        try (FileInputStream fis = new FileInputStream(new File(PROPERTIES_FILE_PATH_CONSUMER))) {
            properties.load(fis);
        } catch (IOException ioe) {
            throw new SubscribeException("ERROR: Cannot open properties file for subscription", ioe);
        }
        if (properties.getProperty("group.id") == null) {
            properties.setProperty("group.id", "group-" + new Random().nextInt(100_000));
        }
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(topics);

        // Update consumerId - consumer relation
        this.consumerIds2consumers.put(registrationId.getConsumerId(), consumer);

        // Update consumerId - topics relation
        this.consumerIds2topics.put(registrationId.getConsumerId(), topics);

        // Update subscriber - consumer relation
        List<String> consumers = this.subscriberIds2consumerIds.get(registrationId.getSubscriberId());
        if (consumers == null) {
            consumers = new LinkedList<>();
        }
        consumers.add(registrationId.getConsumerId());
        this.subscriberIds2consumerIds.put(registrationId.getSubscriberId(), consumers);

        this.consumerIds2subscriberIds.put(registrationId.getConsumerId(), registrationId.getSubscriberId());

        // Return the registration id
        return registrationId;
    }

    public void unsubscribe(RegistrationId registrationId) throws InvalidCredentialsException {
        // Check validity
        if (!isConsumerValid(registrationId)) {
            throw new InvalidCredentialsException();
        }

        // Close internal consumer
        KafkaConsumer<String, String> consumer = this.consumerIds2consumers.remove(registrationId.getConsumerId());
        consumer.close();

        // Update structures
        this.consumerIds2topics.remove(registrationId.getConsumerId());
        this.consumerIds2subscriberIds.remove(registrationId.getConsumerId());

        List<String> consumers = this.subscriberIds2consumerIds.remove(registrationId.getSubscriberId());
        consumers.remove(registrationId.getConsumerId());
        if (!consumers.isEmpty()) {
            this.subscriberIds2consumerIds.put(registrationId.getSubscriberId(), consumers);
        }
    }

    public void updateSubscriptionTopics(RegistrationId registrationId, List<String> newTopics) throws InvalidCredentialsException {
        // Check validity
        if (!isConsumerValid(registrationId)) {
            throw new InvalidCredentialsException();
        }

        // Update internal subscription
        KafkaConsumer<String, String> consumer = this.consumerIds2consumers.get(registrationId.getConsumerId());
        consumer.unsubscribe();
        consumer.subscribe(newTopics);

        // Update structures
        this.consumerIds2topics.put(registrationId.getConsumerId(), newTopics);
    }

    public ConsumerRecords<String, String> poll(RegistrationId registrationId, int timeout) throws InvalidCredentialsException {
        // Check validity
        if (!isConsumerValid(registrationId)) {
            throw new InvalidCredentialsException();
        }

        // Retrieve records from all subscriptions
        KafkaConsumer<String, String> consumer = this.consumerIds2consumers.get(registrationId.getConsumerId());
        return consumer.poll(timeout);
    }

    /*
     * ****************************************************************************************************************
     * PRODUCER METHODS
     * ****************************************************************************************************************
     */

    public RegistrationId announce() throws AnnounceException {
        // Create subscription id
        RegistrationId registrationId = new RegistrationId();

        // Create internal producer
        Properties properties = new Properties();
        try (FileInputStream fis = new FileInputStream(new File(PROPERTIES_FILE_PATH_PRODUCER))) {
            properties.load(fis);
        } catch (IOException ioe) {
            throw new AnnounceException("ERROR: Cannot open properties file for announcement", ioe);
        }
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // Update producerId - producer relation
        this.producerIds2producers.put(registrationId.getProducerId(), producer);

        // Update subscriber - producerId relation
        List<String> producers = this.subscriberIds2producerIds.get(registrationId.getSubscriberId());
        if (producers == null) {
            producers = new LinkedList<>();
        }
        producers.add(registrationId.getProducerId());
        this.subscriberIds2producerIds.put(registrationId.getSubscriberId(), producers);

        this.producerIds2subscriberIds.put(registrationId.getProducerId(), registrationId.getSubscriberId());

        // Return the registration id
        return registrationId;
    }

    public void close(RegistrationId registrationId) throws InvalidCredentialsException {
        // Check validity
        if (!isProducerValid(registrationId)) {
            throw new InvalidCredentialsException();
        }

        // Close internal consumer
        KafkaProducer<String, String> producer = this.producerIds2producers.remove(registrationId.getProducerId());
        producer.close();

        // Update structures
        this.producerIds2subscriberIds.remove(registrationId.getProducerId());

        List<String> producers = this.subscriberIds2producerIds.remove(registrationId.getSubscriberId());
        producers.remove(registrationId.getConsumerId());
        if (!producers.isEmpty()) {
            this.subscriberIds2producerIds.put(registrationId.getSubscriberId(), producers);
        }
    }

    public void publish(RegistrationId registrationId, String topic, String message) throws InvalidCredentialsException {
        // Check validity
        if (!isProducerValid(registrationId)) {
            throw new InvalidCredentialsException();
        }

        // Publish message
        KafkaProducer<String, String> producer = this.producerIds2producers.get(registrationId.getProducerId());
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, message);
        producer.send(record);
    }

    /*
     * ****************************************************************************************************************
     * INTERNAL METHODS
     * ****************************************************************************************************************
     */

    private boolean isConsumerValid(RegistrationId registrationId) {
        List<String> consumers = subscriberIds2consumerIds.get(registrationId.getSubscriberId());
        if (consumers != null) {
            return consumers.contains(registrationId.getConsumerId());
        }

        return false;
    }

    private boolean isProducerValid(RegistrationId registrationId) {
        List<String> producers = subscriberIds2producerIds.get(registrationId.getSubscriberId());
        if (producers != null) {
            return producers.contains(registrationId.getProducerId());
        }

        return false;
    }
}
