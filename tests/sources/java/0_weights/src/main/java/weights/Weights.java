package weights;

import es.bsc.compss.api.COMPSs;


public class Weights {

    /**
     * Test main entry.
     * 
     * @param args System arguments.
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        String content = "content";

        WeightsImpl.genTask1("filename1_1", "filename1_2", "filename1_3", content);
        WeightsImpl.genTask2("filename2_1", "filename2_2", "filename2_3", content);
        COMPSs.barrier();
        WeightsImpl.readFiles2("filename2_1", "filename1_2", "filename1_3");
        WeightsImpl.readFiles1("filename1_1", "filename2_2", "filename2_3");

    }

}
