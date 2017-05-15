package integratedtoolkit.nio.workers.binders.test;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import integratedtoolkit.nio.worker.binders.BindToMap;
import integratedtoolkit.nio.worker.exceptions.InvalidMapException;


public class MapTest {

    @Test
    public void cpuMap() throws InvalidMapException {
        final String lscpuOutput = "Architecture: x86_64 CPU op-mode(s): 32-bit, 64-bit \nByte Order: Little Endian \nCPU(s): 16 \n"
                + "Vendor ID: GenuineIntel \nCPU family: 6 \nModel: 45 \nStepping: 7 \nCPU MHz: 2601.000 \nBogoMIPS: 5199.90 \n"
                + "Virtualization: VT-x \nL1d cache: 32K \nL1i cache: 32K \nL2 cache: 256K \nL3 cache: 20480K";

        final String mapExpected = "0-16";
        String mapGot = BindToMap.processLsCpuOutput(lscpuOutput);

        assertEquals(mapExpected, mapGot);
    }

    @Test
    public void socketMap() throws InvalidMapException {
        final String lscpuOutput = "Architecture: x86_64 CPU op-mode(s): 32-bit, 64-bit \nByte Order: Little Endian \nCPU(s): 16 \n"
                + "On-line CPU(s) list: 0-15 \nThread(s) per core: 1 \nCore(s) per socket: 8 \nSocket(s): 2 \nNUMA node(s): 2 \n"
                + "Vendor ID: GenuineIntel \nCPU family: 6 \nModel: 45 \nStepping: 7 \nCPU MHz: 2601.000 \nBogoMIPS: 5199.90 \n"
                + "Virtualization: VT-x \nL1d cache: 32K \nL1i cache: 32K \nL2 cache: 256K \nL3 cache: 20480K \nNUMA node0 CPU(s): 0-7 \n"
                + "NUMA node1 CPU(s): 8-15\n";

        final String mapExpected = "0-7/8-15";
        String mapGot = BindToMap.processLsCpuOutput(lscpuOutput);

        assertEquals(mapExpected, mapGot);
    }

}
