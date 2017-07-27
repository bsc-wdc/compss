package es.bsc.compss.components;

import es.bsc.compss.types.resources.Worker;
import es.bsc.compss.types.resources.WorkerResourceDescription;
import es.bsc.compss.types.resources.updates.ResourceUpdate;


public interface ResourceUser {

    public <T extends WorkerResourceDescription> void updatedResource(Worker<T> r, ResourceUpdate<T> modification);

}
