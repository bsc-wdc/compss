package dummy.groupService.GroupService.GroupServicePort;

public class GroupService {

    public static class Static {

        public static groupservice.Person getOwner() {
            return null;
        }
    }


    public int getNumWorkers() {
        return -1;
    }

    public groupservice.Person getWorker(int id) {
        return null;
    }

    public double productivity() {
        return -1;
    }

    public void setNumWorkers(int numWorkers) {
    }

    public void setWorker(groupservice.Person worker, int id) {
    }
}
