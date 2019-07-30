
package workerinmaster;

import es.bsc.compss.api.COMPSs;
import java.io.File;


public class Test {

    public static final int PARALLEL_TEST_COUNT = 20;
    public static final int PARALLEL_TEST_MAX_COUNT = 4;

    private static final String INITIAL_CONTENT = "This is the initial content of the file";
    private static final String UPDATED_CONTENT_1 = "This is the updated content 1 of the file";


    private static void masterProducerWorkerConsumerFile() throws Exception {
        System.out.println("Master produces file, worker consumes");
        String fileName = "master_producer_worker_consumer";
        Tasks.createFileWithContentMaster(INITIAL_CONTENT, fileName);
        Tasks.checkFileWithContentWorker(INITIAL_CONTENT, fileName);
        COMPSs.barrier();
        new File(fileName).delete();
        System.out.println("\t OK");
    }

    private static void workerProducerMasterConsumerFile() throws Exception {
        System.out.println("Worker produces file, master consumes");
        String fileName = "worker_producer_master_consumer";
        Tasks.createFileWithContentWorker(INITIAL_CONTENT, fileName);
        Tasks.checkFileWithContentMaster(INITIAL_CONTENT, fileName);
        COMPSs.barrier();
        new File(fileName).delete();
        System.out.println("\t OK");
    }

    private static void masterProducerWorkerConsumerMasterUpdatesFile() throws Exception {
        System.out.println("Master produces file, several workers consume, master updates, worker reads");
        String fileName = "produce_consume_update";
        Tasks.createFileWithContentMaster(INITIAL_CONTENT, fileName);
        for (int i = 0; i < PARALLEL_TEST_COUNT; i++) {
            Tasks.checkFileWithContentWorker(INITIAL_CONTENT, fileName);
        }
        Tasks.checkAndUpdateFileWithContent(INITIAL_CONTENT, UPDATED_CONTENT_1, fileName);
        Tasks.checkFileWithContentWorker(UPDATED_CONTENT_1, fileName);
        COMPSs.barrier();
        System.out.println("\t OK");
    }

    private static void masterProducerWorkerConsumerObject() throws Exception {
        System.out.println("Master produces object, worker consumes");
        StringWrapper sw = Tasks.createObjectWithContentMaster(INITIAL_CONTENT);
        Tasks.checkObjectWithContentWorker(INITIAL_CONTENT, sw);
        COMPSs.barrier();
        System.out.println("\t OK");
    }

    private static void workerProducerMasterConsumerObject() throws Exception {
        System.out.println("Worker produces object, master consumes");
        StringWrapper sw = Tasks.createObjectWithContentWorker(INITIAL_CONTENT);
        Tasks.checkObjectWithContentMaster(INITIAL_CONTENT, sw);
        COMPSs.barrier();
        System.out.println("\t OK");
    }

    private static void masterProducerWorkerConsumerMasterUpdatesObject() throws Exception {
        System.out.println("Master produces object, several workers consume, master updates, worker reads");
        StringWrapper sw = Tasks.createObjectWithContentMaster(INITIAL_CONTENT);
        for (int i = 0; i < PARALLEL_TEST_COUNT; i++) {
            Tasks.checkObjectWithContentWorker(INITIAL_CONTENT, sw);
        }
        Tasks.checkAndUpdateObjectWithContent(INITIAL_CONTENT, UPDATED_CONTENT_1, sw);
        Tasks.checkObjectWithContentWorker(UPDATED_CONTENT_1, sw);
        COMPSs.barrier();
        System.out.println("\t OK");
    }

    /**
     * Main method of the test. It validates the worker in the master process.
     *
     * @param args ignored!
     * @throws Exception Unexpected result obtained during the execution
     */
    public static void main(String[] args) throws Exception {
        masterProducerWorkerConsumerFile();
        workerProducerMasterConsumerFile();
        masterProducerWorkerConsumerMasterUpdatesFile();
        masterProducerWorkerConsumerObject();
        workerProducerMasterConsumerObject();
        masterProducerWorkerConsumerMasterUpdatesObject();
    }

}
