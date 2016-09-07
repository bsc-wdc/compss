package groupService;

import javax.jws.WebMethod;
import javax.jws.WebParam;
import javax.jws.WebService;

import person.Person;


@WebService(serviceName = "GroupService", portName = "GroupServicePort", targetNamespace = "http://groupService")
public class GroupService {

    private static int numWorkers = 10;
    private static Person[] workers = new Person[numWorkers];
    private static final Person owner = new Person("Me", "Me", "12345678A", 20, 16, 4);


    @WebMethod
    public int getNumWorkers() {
        return numWorkers;
    }

    @WebMethod
    public void setNumWorkers(@WebParam(name = "numWorkers") int n) {
        if (n >= 0) {
            numWorkers = n;
            workers = new Person[numWorkers];
            for (int i = 0; i < numWorkers; ++i)
                workers[i] = new Person();
        }
    }

    @WebMethod
    public Person getWorker(@WebParam(name = "id") int id) {
        if ((id >= 0) && (id < numWorkers))
            return new Person(workers[id]);
        return null;
    }

    @WebMethod
    public void setWorker(@WebParam(name = "worker") Person worker, @WebParam(name = "id") int id) {
        if ((id >= 0) && (id < numWorkers)) {
            workers[id] = new Person(worker);
        }
    }

    @WebMethod
    public Person getOwner() {
        Person p = new Person(owner);
        return p;
    }

    @WebMethod
    public double productivity() {
        double acum = owner.productivity();
        int total = 1;
        for (int i = 0; i < numWorkers; ++i) {
            if (workers[i] != null) {
                acum += workers[i].productivity();
                total = total + 1;
            }
        }
        return (acum / total);
    }

}
