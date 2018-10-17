package es.bsc.compss.types.resources;

import es.bsc.compss.log.Loggers;
import java.util.LinkedList;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 *
 * @author flordan
 */
public class ResourcesPool {
    // Log and debug

    protected static final Logger LOGGER = LogManager.getLogger(Loggers.COMM);
    public static final boolean DEBUG = LOGGER.isDebugEnabled();

    // List of all created resources
    private static final List<Resource> AVAILABLE_RESOURCES = new LinkedList<>();

    /**
     * Returns the Resource associated to the given name @name Null if any resource has been registered with the name
     *
     * @name
     *
     * @param name
     * @return
     */
    public static Resource getResource(String name) {
        for (Resource r : AVAILABLE_RESOURCES) {
            if (r.getName().equals(name)) {
                return r;
            }
        }
        LOGGER.warn("Resource with name " + name + " not found");
        return null;
    }

    public static void add(Resource res) {
        AVAILABLE_RESOURCES.add(res);
    }

}
