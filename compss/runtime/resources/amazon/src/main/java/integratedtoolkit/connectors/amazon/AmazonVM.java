package integratedtoolkit.connectors.amazon;

import integratedtoolkit.connectors.VM;
import integratedtoolkit.types.resources.description.CloudMethodResourceDescription;


public class AmazonVM extends VM {

    private int placement;
    private VMType type;

    // PLACEMENTS
    private static final int PLACEMENT_US_EAST = 0;
    private static final int PLACEMENT_US_CAL = 1;
    private static final int PLACEMENT_US_OREGON = 2;
    private static final int PLACEMENT_EUROPE = 3;
    private static final int PLACEMENT_PACIFIC_SINGAPUR = 4;
    private static final int PLACEMENT_PACIFIC_TOKIO = 5;
    private static final int PLACEMENT_SAOPAULO = 6;
    private static final VMType micro = new Micro();
    private static final VMType small = new Small();
    private static final VMType medium = new Medium();
    private static final VMType large = new Large();
    private static final VMType xlarge = new XLarge();


    public AmazonVM(String envId, CloudMethodResourceDescription rd, String type, int placement) {
        super(envId, rd);
        this.placement = placement;
        if (type.equals(micro.getCode())) {
            this.type = micro;
        } else if (type.equals(small.getCode())) {
            this.type = small;
        } else if (type.equals(medium.getCode())) {
            this.type = medium;
        } else if (type.equals(large.getCode())) {
            this.type = large;
        } else if (type.equals(xlarge.getCode())) {
            this.type = xlarge;
        }
    }

    public int getPlacement() {
        return placement;
    }

    public VMType getType() {
        return type;
    }

    public float getPrice() {
        return type.getPrice(placement);
    }

    public boolean isCompatible(String code, String arch) {
        String thisCode = this.type.getCode();
        if (arch != null && arch.equals("i386")) {
            if (code.equals("t1.micro")) {
                return thisCode.equals("m1.micro") || thisCode.equals("m1.small") || thisCode.equals("m1.medium");
            } else if (code.equals("m1.small")) {
                return thisCode.equals("m1.small") || thisCode.equals("m1.medium");
            } else if (code.equals("m1.medium")) {
                return thisCode.equals("m1.medium");
            }
        } else {
            if (code.equals("t1.micro")) {
                return thisCode.equals("m1.micro") || thisCode.equals("m1.small") || thisCode.equals("m1.medium")
                        || thisCode.equals("m1.large") || thisCode.equals("m1.xlarge");
            } else if (code.equals("m1.small")) {
                return thisCode.equals("m1.small") || thisCode.equals("m1.medium") || thisCode.equals("m1.large")
                        || thisCode.equals("m1.xlarge");
            } else if (code.equals("m1.medium")) {
                return thisCode.equals("m1.medium") || thisCode.equals("m1.large") || thisCode.equals("m1.xlarge");
            } else if (code.equals("m1.large")) {
                return thisCode.equals("m1.large") || thisCode.equals("m1.xlarge");
            } else if (code.equals("m1.xlarge")) {
                return thisCode.equals("m1.xlarge");
            }
        }
        return false;
    }

    public static float getPrice(String type, int placement) {
        if (type.equals(micro.getCode())) {
            return micro.getPrice(placement);
        } else if (type.equals(small.getCode())) {
            return small.getPrice(placement);
        } else if (type.equals(medium.getCode())) {
            return medium.getPrice(placement);
        } else if (type.equals(large.getCode())) {
            return large.getPrice(placement);
        } else if (type.equals(xlarge.getCode())) {
            return xlarge.getPrice(placement);
        }
        return 0.0f;
    }

    public static int translatePlacement(String placeCode) {
        if (placeCode.substring(0, 2).compareTo("ap") == 0) {
            if (placeCode.substring(3, 12).compareTo("northeast") == 0) {
                return PLACEMENT_PACIFIC_TOKIO;
            } else {// placeCode.substring(3,7).compareTo("southeast")==0
                return PLACEMENT_PACIFIC_SINGAPUR;
            }
        } else if (placeCode.substring(0, 2).compareTo("eu") == 0) {
            return PLACEMENT_EUROPE;
        } else if (placeCode.substring(0, 2).compareTo("sa") == 0) {
            return PLACEMENT_SAOPAULO;
        } else {// (placeCode.substring(2).compareTo("us")==0)
            if (placeCode.substring(3, 7).compareTo("east") == 0) {
                return PLACEMENT_US_EAST;
            } else {// placeCode.substring(3,7).compareTo("west")==0
                if (placeCode.substring(8, 8).compareTo("1") == 0) {
                    return PLACEMENT_US_CAL;
                } else {
                    return PLACEMENT_US_OREGON;
                }
            }
        }
    }

    public static String classifyMachine(int cpu, float memory, float disk, String imageArchitecture) {
        // System.out.println("Classifying machine with cpu " + cpu + ", memory " + memory + " disk " + disk + ", arch "
        // + imageArchitecture);
        if (imageArchitecture == null || imageArchitecture == "[unassigned]" || imageArchitecture.compareTo("x64") == 0) {
            // System.out.println("In x64");
            if (checkVM(cpu, memory, disk, micro)) {
                // System.out.println("Returning micro");
                return micro.getCode();
            }
            if (checkVM(cpu, memory, disk, small)) {
                // System.out.println("Returning small");
                return small.getCode();
            }
            if (checkVM(cpu, memory, disk, medium)) {
                return medium.getCode();
            }
            if (checkVM(cpu, memory, disk, large)) {
                return large.getCode();
            }
            if (checkVM(cpu, memory, disk, xlarge)) {
                return xlarge.getCode();
            }
        } else { // if (imageArchitecture.compareTo("i386"))
            // System.out.println("In 32");
            if (checkVM(cpu, memory, disk, micro)) {
                // System.out.println("Returning micro");
                return micro.getCode();
            }
            if (checkVM(cpu, memory, disk, small)) {
                // System.out.println("Returning small");
                return small.getCode();
            }
            if (checkVM(cpu, memory, disk, medium)) {
                return medium.getCode();
            }
        }
        // System.out.println("Returning null");
        return null;
    }

    private static boolean checkVM(int cpu, float memory, float disk, VMType vm) {
        return cpu <= vm.getCpucount() && memory <= vm.getMemory() && disk <= vm.getDisk();
    }

    // Comparable interface implementation

    public int compareTo(AmazonVM vm) throws NullPointerException {
        if (vm == null) {
            throw new NullPointerException();
        }

        if (vm.getName().equals(getName())) {
            return 0;
        }

        long now = System.currentTimeMillis();
        int mod1 = (int) (now - getStartTime()) % 3600000; // 1 h in ms
        int mod2 = (int) (now - vm.getStartTime()) % 3600000; // 1 h in ms

        return mod2 - mod1;
    }

    public String toString() {
        return "VM " + " (type = " + type + ", placement = " + placement + super.toString();
    }


    public static abstract class VMType {

        protected String code;
        protected float memory;
        protected int cpuCount;
        protected float disk;
        protected float[] price;


        public String getCode() {
            return code;
        }

        public float getMemory() {
            return memory;
        }

        public int getCpucount() {
            return cpuCount;
        }

        public float getDisk() {
            return disk;
        }

        public float getPrice(int placement) {
            return price[placement];
        }

        public String toString() {
            return code;
        }
    }

    private static class Micro extends VMType {

        public Micro() {
            code = "t1.micro";
            memory = 613.0f;
            cpuCount = 1;
            disk = 40000f; // ?
            price = new float[] { 0.020f, 0.020f, 0.025f, 0.020f, 0.020f, 0.027f, 0.027f };
        }
    }

    private static class Small extends VMType {

        public Small() {
            code = "m1.small";
            memory = 1740.8f;
            cpuCount = 1;
            disk = 163840f;
            price = new float[] { 0.080f, 0.080f, 0.090f, 0.085f, 0.085f, 0.092f, 0.115f };
        }
    }

    private static class Medium extends VMType {

        public Medium() {
            code = "m1.medium";
            memory = 3840f;
            cpuCount = 1;
            disk = 419840f;
            price = new float[] { 0.16f, 0.16f, 0.18f, 0.17f, 0.17f, 0.184f, 0.23f };
        }
    }

    private static class Large extends VMType {

        public Large() {
            code = "m1.large";
            memory = 7680f;
            cpuCount = 2;
            disk = 870400f;
            price = new float[] { 0.32f, 0.32f, 0.36f, 0.34f, 0.34f, 0.368f, 0.46f };
        }
    }

    private static class XLarge extends VMType {

        public XLarge() {
            code = "m1.xlarge";
            memory = 15360f;
            cpuCount = 4;
            disk = 1730560f;
            price = new float[] { 0.64f, 0.64f, 0.72f, 0.68f, 0.68f, 0.736f, 0.92f };
        }
    }
}
