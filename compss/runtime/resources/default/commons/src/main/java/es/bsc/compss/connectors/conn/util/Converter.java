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
package es.bsc.compss.connectors.conn.util;

import es.bsc.compss.types.resources.configuration.MethodConfiguration;
import es.bsc.compss.types.resources.description.CloudImageDescription;
import es.bsc.compss.types.resources.description.CloudInstanceTypeDescription;
import es.bsc.compss.types.resources.description.CloudMethodResourceDescription;

import es.bsc.conn.types.HardwareDescription;
import es.bsc.conn.types.InstallationDescription;
import es.bsc.conn.types.Processor;
import es.bsc.conn.types.SoftwareDescription;
import es.bsc.conn.types.VirtualResource;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;


public class Converter {

    private Converter() {
        // Private constructor to avoid instantiation
    }

    /*
     * COMPSs TO CONN
     */

    /**
     * Returns the hardware description.
     *
     * @param cmrd CloudMethodResource description.
     * @return The hardware description.
     */
    public static HardwareDescription getHardwareDescription(CloudMethodResourceDescription cmrd) {
        List<Processor> processors = Converter.getConnectorProcessors(cmrd.getProcessors());
        int cpuCU = cmrd.getTotalCPUComputingUnits();
        int gpuCU = cmrd.getTotalGPUComputingUnits();
        int fpgaCU = cmrd.getTotalFPGAComputingUnits();
        float memSize = cmrd.getMemorySize();
        String memType = cmrd.getMemoryType();
        float storageSize = cmrd.getStorageSize();
        String storageType = cmrd.getStorageType();
        int timeUnit = cmrd.getPriceTimeUnit();
        float priceUnit = cmrd.getPricePerUnit();
        String imageName = cmrd.getImage().getImageName();
        Map<CloudInstanceTypeDescription, int[]> composition = cmrd.getTypeComposition();
        String instanceType = null;
        for (CloudInstanceTypeDescription type : composition.keySet()) {
            instanceType = type.getName();
            break;
        }

        Map<String, String> imageProp = cmrd.getImage().getProperties();
        // FIXME Using CPU Computing units, should check all units
        return new HardwareDescription(processors, cpuCU, gpuCU, fpgaCU, memSize, memType, storageSize, storageType,
            timeUnit, priceUnit, imageName, instanceType, imageProp);
    }

    /**
     * Returns the software description.
     *
     * @param cmrd CloudMethodResource description.
     * @return The software description.
     */
    public static SoftwareDescription getSoftwareDescription(CloudMethodResourceDescription cmrd) {
        String osType = cmrd.getOperatingSystemType();
        String osDist = cmrd.getOperatingSystemDistribution();
        String osVersion = cmrd.getOperatingSystemVersion();
        List<String> apps = cmrd.getAppSoftware();
        InstallationDescription installDesc = getInstallationDescription(cmrd.getImage().getConfig());

        return new SoftwareDescription(osType, osDist, osVersion, apps, installDesc);
    }

    /**
     * Returns the virtual resource.
     *
     * @param id Image id.
     * @param cmrd CloudMethodResource description.
     * @return The virtual resource.
     */
    public static VirtualResource getVirtualResource(Object id, CloudMethodResourceDescription cmrd) {
        return new VirtualResource((String) id, getHardwareDescription(cmrd), getSoftwareDescription(cmrd),
            cmrd.getImage().getProperties());
    }

    /**
     * Returns the connector processor.
     *
     * @param p Internal processor.
     * @return The connector processor built from the given internal processor information.
     */
    private static Processor getConnectorProcessor(es.bsc.compss.types.resources.components.Processor p) {
        return new Processor(p.getName(), p.getComputingUnits(), p.getType().toString(), p.getInternalMemory(),
            p.getSpeed(), p.getArchitecture(), p.getPropName(), p.getPropValue());
    }

    /**
     * Returns a list of connector processor built from translating all the processors in the given list.
     *
     * @param processorList List of internal processors.
     * @return List of connector processors built from the given list of internal processors.
     */
    private static List<Processor>
        getConnectorProcessors(List<es.bsc.compss.types.resources.components.Processor> processorList) {

        List<Processor> processors = new LinkedList<>();
        for (es.bsc.compss.types.resources.components.Processor p : processorList) {
            processors.add(getConnectorProcessor(p));
        }
        return processors;
    }

    /**
     * Returns the installation description.
     *
     * @param config Method configuration.
     * @return Installation description.
     */
    private static InstallationDescription getInstallationDescription(MethodConfiguration config) {
        InstallationDescription installDesc = new InstallationDescription(config.getAdaptorName(), config.getMinPort(),
            config.getMaxPort(), config.getInstallDir(), config.getAppDir(), config.getClasspath(),
            config.getPythonpath(), config.getLibraryPath(), config.getEnvScript(), config.getPythonInterpreter(),
            config.getWorkingDir(), config.getLimitOfTasks());
        return installDesc;
    }

    /*
     * CONN TO COMPSs
     */

    /**
     * Creates an internal CloudMethodResourceDescription from the given virtual resource description.
     * 
     * @param vr Virtual Resource.
     * @param requested Requested CloudMethodResourceDescription.
     * @return A CloudMethodResource description containing the VR information.
     */
    public static CloudMethodResourceDescription toCloudMethodResourceDescription(VirtualResource vr,
        CloudMethodResourceDescription requested) {

        CloudMethodResourceDescription cmrd = new CloudMethodResourceDescription();
        cmrd.setName(vr.getIp());
        setHardwareInResourceDescription(cmrd, vr.getHd(), requested);
        setSoftwareInResourceDescription(cmrd, vr.getSd(), requested);
        CloudImageDescription cid = getCloudImageDescription(vr.getHd(), vr.getSd(), requested);
        cmrd.setImage(cid);
        cmrd.getImage().getConfig().setMinPort(vr.getSd().getInstallation().getMinPort());
        cmrd.getImage().getConfig().setMaxPort(vr.getSd().getInstallation().getMaxPort());

        return cmrd;
    }

    /**
     * Returns the equivalent internal processor to the given connector processor.
     *
     * @param p Connector processor.
     * @return Internal processor representing the connector processor.
     */
    private static es.bsc.compss.types.resources.components.Processor getCOMPSsProcessor(Processor p) {
        // FIXME Assuming that all processors have type CPU and no mem
        return new es.bsc.compss.types.resources.components.Processor(p.getName(), p.getComputingUnits(), p.getSpeed(),
            p.getArchitecture(), p.getType(), p.getInternalMemory(), p.getPropName(), p.getPropValue());
    }

    /**
     * Returns a list of equivalent internal processors.
     *
     * @param processorList A list of connector processors.
     * @return A list of equivalent internal processors.
     */
    private static List<es.bsc.compss.types.resources.components.Processor>
        getCOMPSsProcessors(List<Processor> processorList) {
        List<es.bsc.compss.types.resources.components.Processor> processors = new LinkedList<>();

        for (Processor p : processorList) {
            processors.add(getCOMPSsProcessor(p));
        }
        return processors;
    }

    /**
     * Returns the cloud image description.
     *
     * @param hd Hardware description.
     * @param sd Software description.
     * @param requested Requested CloudMethodResource description.
     * @return CloudMethodResource description containing the given hardware and software descriptions.
     */
    private static CloudImageDescription getCloudImageDescription(HardwareDescription hd, SoftwareDescription sd,
        CloudMethodResourceDescription requested) {

        CloudImageDescription from = requested.getImage();
        CloudImageDescription cid = new CloudImageDescription(hd.getImageName(), hd.getImageProperties());
        cid.setOperatingSystemType(sd.getOperatingSystemType());
        cid.setOperatingSystemDistribution(sd.getOperatingSystemDistribution());
        cid.setOperatingSystemVersion(sd.getOperatingSystemVersion());
        cid.setAppSoftware(sd.getAppSoftware());
        cid.setPricePerUnit(hd.getPricePerUnit());
        cid.setPriceTimeUnit(hd.getPriceTimeUnit());
        cid.setConfig(from.getConfig());
        cid.setQueues(from.getQueues());
        cid.setSharedDisks(from.getSharedDisks());
        cid.setPackages(from.getPackages());
        return cid;
    }

    /**
     * Sets the hardware inside the resource description.
     *
     * @param cmrd Final CloudMethodResource description.
     * @param hd Hardware to setup.
     * @param requested Requested CloudMethodResource description.
     */
    private static void setHardwareInResourceDescription(CloudMethodResourceDescription cmrd, HardwareDescription hd,
        CloudMethodResourceDescription requested) {

        cmrd.setProcessors(getCOMPSsProcessors(hd.getProcessors()));
        cmrd.setMemorySize(hd.getMemorySize());
        cmrd.setMemoryType(hd.getMemoryType());
        cmrd.setStorageSize(hd.getStorageSize());
        cmrd.setStorageType(hd.getStorageType());
        cmrd.setPricePerUnit(hd.getPricePerUnit());
        cmrd.setPriceTimeUnit(hd.getPriceTimeUnit());
        for (Entry<CloudInstanceTypeDescription, int[]> type : requested.getTypeComposition().entrySet()) {
            cmrd.addInstances(type.getKey(), type.getValue()[0]);
        }
    }

    /**
     * Sets the software inside the resource description.
     *
     * @param cmrd Final CloudMethodResource description.
     * @param sd Software to setup.
     * @param requested Requested CloudMethodResource description.
     */
    private static void setSoftwareInResourceDescription(CloudMethodResourceDescription cmrd, SoftwareDescription sd,
        CloudMethodResourceDescription requested) {

        cmrd.setOperatingSystemType(sd.getOperatingSystemType());
        cmrd.setOperatingSystemDistribution(sd.getOperatingSystemDistribution());
        cmrd.setOperatingSystemVersion(sd.getOperatingSystemVersion());
        cmrd.setAppSoftware(sd.getAppSoftware());
    }

}
