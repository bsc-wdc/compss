package es.bsc.compss.types;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Semaphore;

public class TaskGroup implements AutoCloseable{
    
    // Tasks that access the data
    private final List<Task> tasks;
    
    private String name;
    
    private boolean graphDrawn;
    
    private Semaphore sem;
    
    public TaskGroup (String groupName) {
        this.tasks = new LinkedList<Task>();
        this.graphDrawn = false;
        this.name = groupName;
    }
    
    /**
     * Returns commutative tasks of group
     * 
     * @return
     */
    public List<Task> getTasks () {
        return tasks;
    }

    /**
     * Returns commutative tasks of group
     * 
     * @return
     */
    public String getName() {
        return this.name;
    }
    
    /**
     * Adds task to group
     *
     * @param task
     */
    public void addTask(Task task) {
        tasks.add(task);
    }

    /**
     * Sets the graph of the group as drawn
     *
     */
    public void setGraphDrawn() {
        this.graphDrawn = true;
    }
    
    public void setSemaphore(Semaphore sem) {
        this.sem = sem;
    }
    
    public void releaseSemaphore() {
        this.sem.release();
    }
    
    /**
     * Returns if the graph of the group has been drawn
     *
     * @return
     */
    public boolean getGraphDrawn() {
        return this.graphDrawn;
    }
    
    public void removeTask(Task t) {
        this.tasks.remove(t);
    }
    
    public boolean hasPendingTasks() {
        return !this.tasks.isEmpty();
    }
    
    //Eliminar tasques
    @Override
    public void close() throws Exception {
        
        
    }

}

