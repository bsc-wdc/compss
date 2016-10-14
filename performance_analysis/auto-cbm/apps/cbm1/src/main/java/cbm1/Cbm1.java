package cbm1;

public class Cbm1 {

    private static void usage() {
        System.out.println(":::: Usage: runcompss cbm1.Cbm1 num_Tasks:::: ");
        System.out.println("Exiting cbm1...!");
    }

    public static void main(String[] args) {
        if (args.length < 1) {
            usage();
            return;
        }

        int numTasks = Integer.parseInt(args[0]);

        System.out.println(":::::::::::");
        System.out.println("Number of tasks: {{" + numTasks + "}}");
        System.out.println(":::::::::::");

        System.out.println("");
        System.out.println(":::::::::::");
        System.out.println("Starting cbm1...");

        String a = "";
        for (int i = 0; i < numTasks; ++i) {
            System.out.println("Iteration: " + i);
            a = Cbm1Impl.runTaskI(i);
        }

        // if(a.contains("a")) System.out.println("hajsddjashsdj");
        System.out.println(a);

        System.out.println("Finished cbm1!!!");
        System.out.println(":::::::::::");
        System.out.println("");
    }
    
}
