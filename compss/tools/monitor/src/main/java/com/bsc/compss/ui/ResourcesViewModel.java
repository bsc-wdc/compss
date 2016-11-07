package com.bsc.compss.ui;

import java.util.LinkedList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zkoss.bind.annotation.Command;
import org.zkoss.bind.annotation.Init;
import org.zkoss.bind.annotation.NotifyChange;
import org.zkoss.zul.ListModelList;

import com.bsc.compss.commons.Loggers;


public class ResourcesViewModel {

    private List<Resource> resources;
    private static final Logger logger = LogManager.getLogger(Loggers.UI_VM_RESOURCES);


    @Init
    public void init() {
        resources = new LinkedList<>();
    }

    public List<Resource> getResources() {
        return new ListModelList<Resource>(this.resources);
    }

    @Command
    @NotifyChange("resources")
    public void update(List<String[]> newResourcesData) {
        logger.debug("Updating Resources ViewModel...");
        // Erase all current resources
        resources.clear();

        // Import new resources
        for (String[] dr : newResourcesData) {
            /*
             * Each entry in the new Resource data is of the form:
             *      workerName
             *      totalCPUu
             *      totalGPUu
             *      totalFPGAu
             *      totalOTHERu
             *      memory
             *      disk
             *      status
             *      provider
             *      image
             *      actions
             */
            
            // Change format of some fields

            // Check memSize
            if (dr[5] != null) {
                if (dr[5].startsWith("0.")) {
                    Float memsize = Float.parseFloat(dr[5]);
                    dr[5] = String.valueOf(memsize * 1024) + " MB";
                } else if (!dr[5].isEmpty()) {
                    dr[5] = dr[5] + " GB";
                } else {
                    dr[5] = "-";
                }
            } else {
                dr[5] = "-";
            }

            // Check Disk Size
            if (dr[6] != null) {
                if (dr[6].startsWith("0.")) {
                    Float disksize = Float.parseFloat(dr[6]);
                    dr[6] = String.valueOf(disksize * 1024) + " MB";
                } else if (!dr[6].isEmpty()) {
                    dr[6] = dr[6] + " GB";
                } else {
                    dr[6] = "-";
                }
            } else {
                dr[6] = "-";
            }

            Resource r = new Resource(dr);
            resources.add(r);
        }
        logger.debug("Resources ViewModel updated");
    }

    @Command
    @NotifyChange("resources")
    public void clear() {
        resources.clear();
    }

}
