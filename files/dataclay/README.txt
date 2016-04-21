Tarball contents
================
 |
 |--- producer
 |        `--- producer.properties (file for init the producer app, WARNING: StubsClasspath needs to be set)
 |
 |--- consumer
 |        `--- consumer.properties (file for init  the consumer app, WARNING: StubsClasspath needs to be set)
 |
 |--- client.properties (common properties to connect with WASABI)
 |
 `--- wasabiclient.jar (wasabi client lib)


Usage
=====

1. Set StubsClasspath in {producer, consumer}.properties files in order to specify the absolute path where the stubs are stored.

2. Set WasabiClientConfig in {producer, consumer}.properties files in order to specify the absolute path to client.properties.

3. Define the client.properties common path with DATACLAYCLIENTCONFIG env. variable.

4. In order to use the Severo Ochoa Storage common API your applications must import severo.storage.StorageLib.

5. When calling to "void init(string configfile)", specify the absolute path of the {producer, consumer}.properties file.
