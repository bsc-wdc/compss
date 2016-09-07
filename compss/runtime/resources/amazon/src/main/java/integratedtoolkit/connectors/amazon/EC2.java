package integratedtoolkit.connectors.amazon;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.AuthorizeSecurityGroupIngressRequest;
import com.amazonaws.services.ec2.model.CreateSecurityGroupRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.DescribeKeyPairsResult;
import com.amazonaws.services.ec2.model.DescribeSecurityGroupsResult;
import com.amazonaws.services.ec2.model.ImportKeyPairRequest;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceState;
import com.amazonaws.services.ec2.model.IpPermission;
import com.amazonaws.services.ec2.model.KeyPairInfo;
import com.amazonaws.services.ec2.model.Placement;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;
import com.amazonaws.services.ec2.model.SecurityGroup;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;

import integratedtoolkit.connectors.AbstractSSHConnector;
import integratedtoolkit.connectors.ConnectorException;
import integratedtoolkit.types.resources.components.Processor;
import integratedtoolkit.types.resources.description.CloudMethodResourceDescription;

import java.io.BufferedReader;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


public class EC2 extends AbstractSSHConnector {

    private static String accessKeyId;
    private static String secretKeyId;
    private static String keyPairName;
    private static String keyLocation;
    private static String securityGroupName;
    private static String placementCode;
    private static int placement;

    private static String MAX_VM_CREATION_TIME = "10"; // Minutes
    private static AmazonEC2Client client;


    public EC2(String name, HashMap<String, String> h) {
        super(name, h);
        // Connector parameters
        accessKeyId = h.get("ec2-cred-access-key-id");
        secretKeyId = h.get("ec2-cred-secret-key-id");

        securityGroupName = h.get("ec2-security-group-name");
        placementCode = h.get("ec2-placement");
        placement = AmazonVM.translatePlacement(placementCode);

        // Prepare Amazon for first use
        client = new AmazonEC2Client(new BasicAWSCredentials(accessKeyId, secretKeyId));
        if (h.get("max-vm-creation-time") != null) {
            MAX_VM_CREATION_TIME = h.get("max-vm-creation-time");
        }
        boolean found = false;

        DescribeKeyPairsResult dkpr = client.describeKeyPairs();
        for (KeyPairInfo kp : dkpr.getKeyPairs()) {
            if (kp.getKeyName().compareTo(keyPairName) == 0) {
                found = true;
                break;
            }
        }
        if (!found) {
            try {
                ImportKeyPairRequest ikpReq = new ImportKeyPairRequest(keyPairName, getPublicKey());
                client.importKeyPair(ikpReq);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        found = false;
        DescribeSecurityGroupsResult dsgr = client.describeSecurityGroups();
        for (SecurityGroup sg : dsgr.getSecurityGroups()) {
            if (sg.getGroupName().compareTo(securityGroupName) == 0) {
                found = true;
                break;
            }
        }
        if (!found) {
            try {
                CreateSecurityGroupRequest sg = new CreateSecurityGroupRequest(securityGroupName, "Default compss security");
                client.createSecurityGroup(sg);
                IpPermission ipp = new IpPermission();
                ipp.setToPort(22);
                ipp.setFromPort(22);
                ipp.setIpProtocol("tcp");
                ArrayList<String> ipranges = new ArrayList<String>();
                ipranges.add("0.0.0.0/0");
                ipp.setIpRanges(ipranges);
                ArrayList<IpPermission> list_ipp = new ArrayList<IpPermission>();
                list_ipp.add(ipp);
                AuthorizeSecurityGroupIngressRequest asgi = new AuthorizeSecurityGroupIngressRequest(securityGroupName, list_ipp);
                client.authorizeSecurityGroupIngress(asgi);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }

    // COST INTERFACE IMPLEMENTATION
    /*
     * public Float getTotalCost() { float runningCost = 0.0f; for (java.util.Map.Entry<String, Long> e :
     * IPToStart.entrySet()) { float machineCost = 0.0f; String IP = e.getKey(); String instanceType = IPToType.get(IP);
     * if (instanceType.compareTo(smallCode) == 0) { machineCost = (((System.currentTimeMillis() - e.getValue()) /
     * 3600000) + 1) * smallPrice[placement]; } else if (instanceType.compareTo(largeCode) == 0) { machineCost =
     * (((System.currentTimeMillis() - e.getValue()) / 3600000) + 1) * largePrice[placement]; } else if
     * (instanceType.compareTo(xlargeCode) == 0) { machineCost = (((System.currentTimeMillis() - e.getValue()) /
     * 3600000) + 1) * xlargePrice[placement]; } runningCost += machineCost; } return accumulatedCost + runningCost; }
     * 
     * public Float currentCostPerHour() { return smallCount * smallPrice[placement] + largeCount *
     * largePrice[placement] + xlargeCount * xlargePrice[placement]; }
     * 
     * public Float getMachineCostPerHour(ResourceDescription rc) { int procs = rc.getProcessorCoreCount(); float mem =
     * rc.getMemoryPhysicalSize(); float disk = rc.getStorageElemSize(); CloudImageDescription diskImage =
     * rc.getImage(); String instanceCode = classifyMachine(procs, mem, disk, diskImage.getName()); if
     * (instanceCode.compareTo(smallCode) == 0) { return smallPrice[placement]; } if (instanceCode.compareTo(largeCode)
     * == 0) { return largePrice[placement]; } if (instanceCode.compareTo(xlargeCode) == 0) { return
     * xlargePrice[placement]; } return null; }
     */

    public void destroy(Object worker) throws ConnectorException {
        try {
            RunInstancesResult workerInstance = (RunInstancesResult) worker;
            String instanceId = workerInstance.getReservation().getInstances().get(0).getInstanceId();
            ArrayList<String> instanceIds = new ArrayList<String>();
            instanceIds.add(instanceId);
            TerminateInstancesRequest tir = new TerminateInstancesRequest(instanceIds);
            client.terminateInstances(tir);
        } catch (Exception e) {
            throw new ConnectorException(e);
        }
    }

    private static String getPublicKey() throws Exception {
        BufferedReader br = null;
        try {
            br = new java.io.BufferedReader(new java.io.FileReader(keyLocation + File.separator + keyPairName + ".pub"));
            StringBuilder key = new StringBuilder();
            String sb = br.readLine();
            while (sb != null) {
                key.append(sb).append("\n");
                sb = br.readLine();
            }
            return key.toString();
        } catch (Exception e) {
            throw e;
        } finally {
            if (br != null) {
                br.close();
            }
        }
    }

    private RunInstancesResult createMachine(String instanceCode, String diskImage) throws InterruptedException {
        // Create
        RunInstancesRequest runInstancesRequest = new RunInstancesRequest(diskImage, 1, 1);
        Placement placement = new Placement(placementCode);
        runInstancesRequest.setPlacement(placement);
        runInstancesRequest.setInstanceType(instanceCode);
        runInstancesRequest.setKeyName(keyPairName);
        ArrayList<String> groupId = new ArrayList<String>();
        groupId.add(securityGroupName);
        runInstancesRequest.setSecurityGroups(groupId);

        return client.runInstances(runInstancesRequest);
    }

    public String getId() {
        return "amazon.ec2";
    }

    public String getDefaultUser() {
        return "ec2-user";
    }

    public String getDefaultWDir() {
        return "/home/ec2-user/wDir/";
    }

    public String getDefaultIDir() {
        return "/home/ec2-user/iDir/";
    }

    @Override
    public Object create(String name, CloudMethodResourceDescription requested) throws ConnectorException {
        // Get architecture (workarround for multi-architectures)
        String arch = CloudMethodResourceDescription.UNASSIGNED_STR;
        List<String> available_archs = requested.getArchitectures();
        if (available_archs != null && !available_archs.isEmpty()) {
            arch = available_archs.get(0);
        }

        logger.debug("Requesting machine creation " + name + ", with arch " + arch + ", description: " + requested);
        String instanceCode = AmazonVM.classifyMachine(requested.getTotalComputingUnits(), requested.getMemorySize() * 1024f,
                requested.getStorageSize() * 1024f, arch);
        try {
            RunInstancesResult res = createMachine(instanceCode, requested.getImage().getImageName());
            logger.debug("Request for VM creation sent");
            return res;
        } catch (Exception e) {
            logger.error("Error creating machine " + name, e);
            throw new ConnectorException(e);
        }

    }

    @Override
    public CloudMethodResourceDescription waitUntilCreation(Object vm, CloudMethodResourceDescription requested) throws ConnectorException {
        CloudMethodResourceDescription granted = new CloudMethodResourceDescription();
        Integer poll_time = 5; // Seconds
        Integer polls = 0;
        int errors = 0;

        DescribeInstancesResult dir = null;

        InstanceState status = ((RunInstancesResult) vm).getReservation().getInstances().get(0).getState();
        // Thread.sleep(30000);
        // Thread.sleep(poll_time * 1000);

        // Valid values: 0 (pending) | 16 (running) | 32 (shutting-down) | 48 (terminated) | 64 (stopping) | 80
        // (stopped)
        while (status.getCode() == 0) {
            try {
                // Thread.sleep(10000);
                Thread.sleep(poll_time * 1000);
                if (poll_time * polls >= Integer.parseInt(MAX_VM_CREATION_TIME) * 60) {
                    throw new ConnectorException("Maximum VM creation time reached.");
                }
                polls++;
                DescribeInstancesRequest describeInstancesRequest = new DescribeInstancesRequest();
                ArrayList<String> l = new ArrayList<String>();
                l.add(((RunInstancesResult) vm).getReservation().getInstances().get(0).getInstanceId());
                describeInstancesRequest.setInstanceIds(l);
                dir = client.describeInstances(describeInstancesRequest);
                status = dir.getReservations().get(0).getInstances().get(0).getState();
                errors = 0;
            } catch (Exception e) {
                errors++;
                if (errors == 3) {
                    throw new ConnectorException(e);
                }
            }
        }

        Instance instance = dir.getReservations().get(0).getInstances().get(0);

        String instanceType = instance.getInstanceType();
        String instanceId = instance.getInstanceId();
        String ip = instance.getPublicIpAddress();
        granted.setName(ip);
        AmazonVM vmInfo = new AmazonVM(instanceId, granted, instanceType, placement);

        granted = requested.copy();

        // Workaround for multi-processor
        List<Processor> procs = granted.getProcessors();
        if (procs != null && !procs.isEmpty()) {
            procs.get(0).setComputingUnits(vmInfo.getType().getCpucount());
        } else {
            Processor p = new Processor();
            p.setComputingUnits(vmInfo.getType().getCpucount());
            granted.addProcessor(p);
        }

        float memorySize = vmInfo.getType().getMemory() / 1024f;
        granted.setMemorySize(memorySize);

        float homeSize = vmInfo.getType().getDisk() / 1024f;
        granted.setStorageSize(homeSize);

        granted.setOperatingSystemType("Linux");
        granted.setType(instanceType);
        granted.setValue(getMachineCostPerHour(granted));

        float oneHourCost = AmazonVM.getPrice(instanceType, vmInfo.getPlacement());
        granted.setPricePerUnit(oneHourCost);
        granted.setPriceTimeUnit(60); // Minutess

        return granted;
    }

    @Override
    public float getMachineCostPerTimeSlot(CloudMethodResourceDescription rc) {
        return AmazonVM.getPrice(rc.getType(), placement);
    }

    @Override
    public long getTimeSlot() {
        return ONE_HOUR;
    }

    @Override
    protected void close() {
        // Nothing to do;
    }

}
