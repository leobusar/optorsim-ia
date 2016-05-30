package org.edg.data.replication.optorsim;

import org.edg.data.replication.optorsim.infrastructure.GridJob;

/**
 * The ResourceBroker runs a thread which submits jobs to the 
 * Computing Elements. Users submit jobs to the RB calling
 * {@link #submitJob(GridJob)} then the RB decides to which
 * CE the job will be submitted according to the scheduling
 * algorithm defined in the parameters file. This can be
 * one of the following options:
 * <p>
 * <ol><li><b>Random:</b> The job is submitted to any
 * random Computing Element which will run the job
 * according to its site policy.
 * <li><b>Queue length:</b> The job is scheduled to the Computing
 * Element with the smallest queue of jobs.
 * <li><b>Access cost:</b> The job is scheduled to the Computing
 * Element with the smallest access cost, that is the smallest
 * estimated time to access all the files required.
 * <li><b>Queue access cost:</b>  The job is scheduled to the Computing
 * Element with the smallest queue  access cost, that is the smallest
 * estimated time to access all the files required by all the jobs
 * in the queue of the CE.
 * </ol>
 * A Resource Broker class is implemented for each of these scheduling
 * algorithms.
 * <p>
 * Copyright (c) 2004 CERN, ITC-irst, PPARC, on behalf of the EU DataGrid.
 * For license conditions see LICENSE file or
 * <a href="http://www.edg.org/license.html">http://www.edg.org/license.html</a>
 * <p>
 * @since JDK1.4
 */
public interface ResourceBroker extends Runnable {
	/**
	 * Stops the ResourceBroker from submitting any jobs until the
	 * unPauseRB() method is called.
	 */
	public void pauseRB();
	/**
	 * Starts the ResourceBroker going again after it has been paused.
	 */
	public void unPauseRB();
    /**
     * Submit a job to the Resource Broker.
     */
    public void submitJob(GridJob job);
    /**
     * Tell the RB no more jobs will be submitted.
     */
    public void shutDownRB();
}
