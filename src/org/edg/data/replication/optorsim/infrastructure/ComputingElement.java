package org.edg.data.replication.optorsim.infrastructure;

import org.edg.data.replication.optorsim.JobHandler;  // Is this going to work?

/** 
 * This interface defined the functionality of a ComputingElement (CE), which 
 * processes GridJobs.
 * <p>
 * Copyright (c) 2002 CERN, ITC-irst, PPARC, on behalf of the EU DataGrid.
 * For license conditions see LICENSE file or
 * <a href="http://www.edg.org/license.html">http://www.edg.org/license.html</a>
 * <p>
 * @since JDK1.4
*/
public interface ComputingElement extends Runnable {

    /**
     * The site this CE is on.
     * @return The site this CE is on.
     */
    GridSite getSite();
    
    /**
     * The number of worker nodes this CE has.
     * @return number of worker nodes
     */
    int getWorkerNodes();
    
    /**
     * The current state of the CE.
     * @return true if CE is running a job, false otherwise;
     */
    boolean active();
	
     /**
     * GUI calls this method to pause the ComputingElement
     * threads when pause button is pressed.
     */
    void pauseCE();

	
    /**
     * GUI calls this method to unpause the ComputingElement
     * threads when continue button is pressed.
     */
    void unpauseCE();

	/**
	 * @return The percentage of time the ce was running jobs.
	 */
	Statistics getStatistics();

	/**
	 * Method that returns a CE's job handler
	 * @return The JobHandler for this CE
	 */    
    JobHandler getJobHandler();
    
    /**
     * Method to expose CE's ID by a simple test. 
     * @param index the CE ID we are looking for
     * @return true if this CE is indexed by the number index
     */
	boolean iAm( int index);
	
    /**
     * The ResourceBroker calls this method when it has
     * distributed all the jobs to shut down the ComputingElement
     * threads.
     */
    void shutDownCE();
}
