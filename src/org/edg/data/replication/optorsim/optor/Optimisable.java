package org.edg.data.replication.optorsim.optor;

import org.edg.data.replication.optorsim.infrastructure.ComputingElement;
import org.edg.data.replication.optorsim.infrastructure.DataFile;

/**
 * This interface must be implemented by all replica optimiser classes.
 * Each optimiser should define its own implementation of {@link #getBestFile}.
 * This is the method called whenever a file request is made. It determines
 * where the best replica of a file is and most implementations may perform
 * replication of files within this method.
 * <p>
 * Copyright (c) 2004 CERN, ITC-irst, PPARC, on behalf of the EU DataGrid.
 * For license conditions see LICENSE file or
 * <a href="http://www.edg.org/license.html">http://www.edg.org/license.html</a>
 * <p>
 * @since JDK1.4
 */
public interface Optimisable {

    /**
     * Returns the cost for accessing the files whose names are 
     * specified by the array lfn from the Computing Element
     * specified by ce.
     * @param lfn The names of the files required for the job.
     * @param ce The Computing Element for which the access cost
     * is calculated.
     * @param fileFraction The fractions of each file required for
     * the job.
     * @return The cost in terms of time to access all the best
     * replicas needed for the job.
     */
    public float getAccessCost(String[] lfn,
				ComputingElement ce,
				float[] fileFraction);
    /**
     * Non-blocking getBestFile(). Not yet implemented in OptorSim.
     */
    public void initFilePrefetch(String[]  lfn,
				 ComputingElement ce);
    
    /**
     * Cancels initFilePrefetch(). Not yet implemented in OptorSim.
     */
    public void cancelFilePrefetch(String[] lfn,
				   ComputingElement ce);

    /**
     * Returns a {@link DataFile} array, possibly after some
     * replication has occurred. Computing Elements call this method
     * to find the best replicas of each file to access.
     * @param lfn The name of the files the CE needs to access.
     * @param fileFraction The fraction of each file the CE uses.
     * This can be important in deciding whether it is better to
     * replicate a file or read it remotely.
     * @return The DataFiles the CE will access.
     */
    public DataFile[] getBestFile(String[] lfn,
				  float[] fileFraction);

}







