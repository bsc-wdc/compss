/*
 *  Copyright 2002-2025 Barcelona Supercomputing Center (www.bsc.es)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package es.bsc.compss.binders.test;

import static org.junit.Assert.assertEquals;

import es.bsc.compss.binders.BindToMap;
import es.bsc.compss.types.execution.exceptions.InvalidMapException;

import org.junit.Test;


public class MapTest {

    @Test
    public void cpuMap() throws InvalidMapException {
        final String lscpuOutput =
            "Architecture: x86_64 CPU op-mode(s): 32-bit, 64-bit \nByte Order: Little Endian \n" + "CPU(s): 16 \n"
                + "Vendor ID: GenuineIntel \nCPU family: 6 \nModel: 45 \nStepping: 7 \nCPU MHz: 2601.000 \n"
                + "BogoMIPS: 5199.90 \n"
                + "Virtualization: VT-x \nL1d cache: 32K \nL1i cache: 32K \nL2 cache: 256K \nL3 cache: 20480K";

        final String mapExpected = "0-16";
        String mapGot = BindToMap.processLsCpuOutput(lscpuOutput);

        assertEquals(mapExpected, mapGot);
    }

    @Test
    public void socketMap() throws InvalidMapException {
        final String lscpuOutput = "Architecture: x86_64 CPU op-mode(s): 32-bit, 64-bit \n"
            + "Byte Order: Little Endian \nCPU(s): 16 \nOn-line CPU(s) list: 0-15 \n"
            + "Thread(s) per core: 1 \nCore(s) per socket: 8 \nSocket(s): 2 \nNUMA node(s): 2 \n"
            + "Vendor ID: GenuineIntel \nCPU family: 6 \nModel: 45 \nStepping: 7 \nCPU MHz: 2601.000 \n"
            + "BogoMIPS: 5199.90 \nVirtualization: VT-x \nL1d cache: 32K \nL1i cache: 32K \nL2 cache: 256K \n"
            + "L3 cache: 20480K \nNUMA node0 CPU(s): 0-7 \nNUMA node1 CPU(s): 8-15\n";

        final String mapExpected = "0-7/8-15";
        String mapGot = BindToMap.processLsCpuOutput(lscpuOutput);

        assertEquals(mapExpected, mapGot);
    }

    @Test
    public void complexSocketMap() throws InvalidMapException {
        final String lscpuOutput = "Architecture:          x86_64\nCPU op-mode(s):        32-bit, 64-bit\n"
            + "Byte Order:            Little Endian\nCPU(s):                56\nOn-line CPU(s) list:   0-55\n"
            + "Thread(s) per core:    2\nCore(s) per socket:    14\n"
            + "Socket(s):             2\nNUMA node(s):          2\n"
            + "Vendor ID:             GenuineIntel\nCPU family:            6\nModel:                 79\n"
            + "Model name:            Intel(R) Xeon(R) CPU E5-2690 v4 @ 2.60GHz\nStepping:              1\n"
            + "CPU MHz:               1200.328\nCPU max MHz:           3500.0000\n"
            + "CPU min MHz:           1200.0000\n"
            + "BogoMIPS:              5188.20\nVirtualization:        VT-x\nL1d cache:             32K\n"
            + "L1i cache:             32K\n"
            + "L2 cache:              256K\nL3 cache:              35840K\nNUMA node0 CPU(s):     0-13,28-41\n"
            + "NUMA node1 CPU(s):     14-27,42-55\nFlags:                 fpu vme de pse tsc msr pae mce cx8"
            + " apic sep mtrr pge"
            + " mca cmov pat pse36 clflush dts acpi mmx fxsr sse sse2 ss ht tm pbe syscall nx pdpe1gb"
            + " rdtscp lm constant_tsc"
            + " arch_perfmon pebs bts rep_good nopl xtopology nonstop_tsc aperfmperf eagerfpu pni pclmulqdq"
            + " dtes64 monitor ds_cpl"
            + " vmx smx est tm2 ssse3 sdbg fma cx16 xtpr pdcm pcid dca sse4_1 sse4_2 x2apic movbe popcnt"
            + " tsc_deadline_timer aes"
            + " xsave avx f16c rdrand lahf_lm abm 3dnowprefetch ida arat epb pln pts dtherm intel_pt"
            + " tpr_shadow vnmi flexpriority"
            + " ept vpid fsgsbase tsc_adjust bmi1 hle avx2 smep bmi2 erms invpcid rtm cqm rdseed adx smap"
            + " xsaveopt cqm_llc cqm_occup_llc";

        final String mapExpected = "0-13,28-41/14-27,42-55";
        String mapGot = BindToMap.processLsCpuOutput(lscpuOutput);

        assertEquals(mapExpected, mapGot);
    }

}
