package org.edg.data.replication.optorsim;

import org.edg.data.replication.optorsim.infrastructure.GridJob;

/**
 * The Users interface represents the human users of the Grid who
 * submit jobs to it. The run() method loops for the number of jobs
 * to run, calling {@link #getNextJob()},
 * {@link ResourceBroker#submitJob(GridJob)} then {@link #waitForNextJob()}
 * for each job. Users implementations should define how long
 * to wait between submitting jobs using {@link #waitForNextJob()}
 * and what the next job to submit will be using
 * {@link #getNextJob()}.
 * <p/>
 * Copyright (c) 2004 CERN, ITC-irst, PPARC, on behalf of the EU DataGrid.
 * For license conditions see LICENSE file or <a
 * href="http://www.edg.org/license.html">http://www.edg.org/license.html</a>
 * <p/>
 * 
 * @since JDK1.4
 */
public interface Users extends Runnable{

    /**
     * Called after submitting a job to create a delay between jobs.
     */
    void waitForNextJob();

    /**
     * Returns the next GridJob that is to be submitted to the RB.
     * @return The next job to submit to the RB.
     */
    GridJob getNextJob();

    /**
     * Stop the users from submitting jobs.
     */
    void pauseUsers();

    /**
     * Start the users submitting jobs again.
     */
    void unPauseUsers();
}
