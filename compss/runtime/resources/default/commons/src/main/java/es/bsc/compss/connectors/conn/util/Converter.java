package es.bsc.compss.connectors.conn.util;

import es.bsc.conn.types.HardwareDescription;
import es.bsc.conn.types.InstallationDescription;
import es.bsc.conn.types.Processor;
import es.bsc.conn.types.SoftwareDescription;
import es.bsc.conn.types.VirtualResource;
import es.bsc.compss.types.resources.configuration.MethodConfiguration;
import es.bsc.compss.types.resources.description.CloudImageDescription;
import es.bsc.compss.types.resources.description.CloudInstanceTypeDescription;
import es.bsc.compss.types.resources.description.CloudMethodResourceDescription;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;


public class Converter {

    private Converter() {
        // Private constructor to avoid instantiation
    }

    /**
     *********************************************************************************************************************
     *************************************** COMPSs TO CONN **************************************************************
     * *******************************************************************************************************************
     */

    /**
     * Returns the hardware description
     * 
     * @param cmrd
     * @return
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
        return new HardwareDescription(processors, cpuCU, gpuCU, fpgaCU, memSize, memType, storageSize, storageType, timeUnit, priceUnit,
                imageName, instanceType, imageProp);
    }

    /**
     * Returns the software description
     * 
     * @param cmrd
     * @return
     */
    public static SoftwareDescription getSoftwareDescription(CloudMethodResourceDescription cmrd) {
        String osType = cmrd.getOperatingSystemType();
        String osDist = cmrd.getOperatingSystemDistribution();
        String osVersion = cmrd.getOperatingSystemVersion();
        List<String> apps = cmrd.getAppSoftware();
        CloudImageDescription cid = cmrd.getImage();
        MethodConfiguration mc = cid.getConfig();
        InstallationDescription installDesc = getInstallationDescription(mc);
        return new SoftwareDescription(osType, osDist, osVersion, apps, installDesc);
    }

    /**
     * Returns the virtual resource
     * 
     * @param id
     * @param cmrd
     * @return
     */
    public static VirtualResource getVirtualResource(Object id, CloudMethodResourceDescription cmrd) {
        return new VirtualResource((String) id, getHardwareDescription(cmrd), getSoftwareDescription(cmrd),
                cmrd.getImage().getProperties());
    }

    /**
     * Returns the connector processor
     * 
     * @param p
     * @return
     */
    private static Processor getConnectorProcessor(es.bsc.compss.types.resources.components.Processor p) {
        return new Processor(p.getName(), p.getComputingUnits(), p.getType(), p.getInternalMemory(), p.getSpeed(), p.getArchitecture(),
                p.getPropName(), p.getPropValue());
    }

    /**
     * Returns the connector processor
     * 
     * @param processorList
     * @return
     */
    private static List<Processor> getConnectorProcessors(List<es.bsc.compss.types.resources.components.Processor> processorList) {
        List<Processor> processors = new LinkedList<>();
        for (es.bsc.compss.types.resources.components.Processor p : processorList) {
            processors.add(getConnectorProcessor(p));
        }
        return processors;
    }

    /**
     * Returns the installation description
     * 
     * @param config
     * @return
     */
    private static InstallationDescription getInstallationDescription(MethodConfiguration config) {
        return new InstallationDescription(config.getInstallDir(), config.getAppDir(), config.getClasspath(), config.getPythonpath(),
                config.getLibraryPath(), config.getWorkingDir(), config.getLimitOfTasks());
    }

    /**
     *********************************************************************
     ************************ CONN TO COMPSs *****************************
     * *********************************************************************
     */
    /**
     *
     * @param vr
     * @param requested
     * @return
     */
    public static CloudMethodResourceDescription toCloudMethodResourceDescription(VirtualResource vr,
            CloudMethodResourceDescription requested) {
        CloudMethodResourceDescription cmrd = new CloudMethodResourceDescription();
        cmrd.setName(vr.getIp());
        setHardwareInResourceDescription(cmrd, vr.getHd(), requested);
        setSoftwareInResourceDescription(cmrd, vr.getSd(), requested);
        CloudImageDescription cid = getCloudImageDescription(vr.getHd(), vr.getSd(), requested);
        cmrd.setImage(cid);
        return cmrd;
    }

    /**
     * Returns the COMPSs processor
     * 
     * @param p
     * @return
     */
    private static es.bsc.compss.types.resources.components.Processor getCOMPSsProcessor(Processor p) {
        // FIXME Assuming that all processors have type CPU and no mem
        return new es.bsc.compss.types.resources.components.Processor(p.getName(), p.getComputingUnits(), p.getSpeed(), p.getArchitecture(),
                p.getType(), p.getInternalMemory(), p.getPropName(), p.getPropValue());
    }

    /**
     * Returns the COMPSs processors
     * 
     * @param processorList
     * @return
     */
    private static List<es.bsc.compss.types.resources.components.Processor> getCOMPSsProcessors(List<Processor> processorList) {
        List<es.bsc.compss.types.resources.components.Processor> processors = new LinkedList<>();

        for (Processor p : processorList) {
            processors.add(getCOMPSsProcessor(p));
        }
        return processors;
    }

    /**
     * Returns the cloud image description
     * 
     * @param hd
     * @param sd
     * @param requested
     * @return
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
     * Sets the hardware inside the resource description
     * 
     * @param cmrd
     * @param hd
     * @param requested
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
     * Sets the software inside the resource description
     * 
     * @param cmrd
     * @param sd
     * @param requested
     */
    private static void setSoftwareInResourceDescription(CloudMethodResourceDescription cmrd, SoftwareDescription sd,
            CloudMethodResourceDescription requested) {

        cmrd.setOperatingSystemType(sd.getOperatingSystemType());
        cmrd.setOperatingSystemDistribution(sd.getOperatingSystemDistribution());
        cmrd.setOperatingSystemVersion(sd.getOperatingSystemVersion());
        cmrd.setAppSoftware(sd.getAppSoftware());
    }

}
