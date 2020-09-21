package priorityTasks;

public class PriorityTasks {

    public static void main(String[] args) {
        // Get Execution Parameters
        String fileName = "execution_task_order";
        if (args.length != 2) {
            System.out.println("[ERROR] Bad number of parameters.");
            System.out.println("    Usage:  priorityTasks <numNormalTasks> <numPriorityTasks>");
            System.exit(-1);
        }
        int numberNormalTasks = Integer.parseInt(args[0]);
        int numberPriorityTasks = Integer.parseInt(args[1]);

        System.out.println("[LOG] Number of normal tasks created: " + String.valueOf(numberNormalTasks));
        System.out.println("[LOG] Number of priority tasks created: " + String.valueOf(numberPriorityTasks));

        // Creating tasks
        System.out.println("[LOG] Creating Normal tasks");
        for (int i = 0; i < numberNormalTasks; ++i) {
            PriorityTasksImpl.normalTask(fileName);
        }
        System.out.println("[LOG] Creating Priority tasks");
        for (int i = 0; i < numberPriorityTasks; ++i) {
            PriorityTasksImpl.priorityTask(fileName);
        }

        System.out.println("[LOG] All tasks created.");
        System.out.println("[LOG] No more jobs for main. Waiting all tasks to finish.");
    }
}
