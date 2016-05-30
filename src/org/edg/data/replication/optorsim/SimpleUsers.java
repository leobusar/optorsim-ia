package org.edg.data.replication.optorsim;

import org.edg.data.replication.optorsim.infrastructure.GridContainer;
import org.edg.data.replication.optorsim.infrastructure.GridJob;
import org.edg.data.replication.optorsim.infrastructure.OptorSimParameters;
import org.edg.data.replication.optorsim.time.GridTime;
import org.edg.data.replication.optorsim.time.GridTimeFactory;

/**
 * SimpleUsers sleep for a set length of time defined in the parameters
 * file. Jobs are selected according to the probabilities given in the
 * job configuration file.
 * <p/>
 * Copyright (c) 2004 CERN, ITC-irst, PPARC, on behalf of the EU DataGrid.
 * For license conditions see LICENSE file or <a
 * href="http://www.edg.org/license.html">http://www.edg.org/license.html</a>
 * <p/>
 * 
 * @since JDK1.4
 */
public class SimpleUsers implements Users {

    private long _numjobs;
    private boolean _paused = false;
    protected OptorSimParameters _params;
    protected GridTime _time;
    private ResourceBroker _rb;

    public SimpleUsers () {
        _params = OptorSimParameters.getInstance();
        _time = GridTimeFactory.getGridTime();
        _numjobs = _params.getNoOfJobs();
        _rb = ResourceBrokerFactory.getInstance();
    }

    /**
     * While running, jobs are submitted to the ResourceBroker at regular
     * intervals until all jobs are submitted.
     */
    public void run() {

        // run until all the jobs have been submitted
        for(int jobCount=0; jobCount <_numjobs; jobCount++) {
            
            checkIfPaused();

			waitForNextJob();

            // submit the job
            GridJob job = getNextJob();
            _rb.submitJob(job);
            job.scheduled();

            OptorSimOut.println("Users> Submitted job "+(jobCount+1)+" of "+
                       _numjobs+" to RB at " + _time.getDate());
        }

        _rb.shutDownRB();
    }

    /**
     * Sleeps for a set time defined in the parameters file.
     */
    public void waitForNextJob() {
   // 	System.out.println(this.toString()+"> waiting for next job...");
    	_time.gtSleep( _params.getJobDelay());
    }

    /**
     * Returns a new GridJob generated according to the probabilities
     * defined for each job in the job configuration file.
     */
    public GridJob getNextJob() {
        return GridContainer.getInstance().randomJob();
    }

    /**
     * Method that checks if we are paused and if we are,
     * waits until unPauseUsers() is called.
     */
    private void checkIfPaused() {
		while(_paused) {
            _time.gtWait(this);
        }
    }

    /**
     * Stops the Users from submitting any jobs until the
     * unPauseUsers() method is called.
     */
    public void pauseUsers() {
		_paused = true;
    }

    /**
     * Starts the Users going again after being paused.
     */
    public void unPauseUsers() {
		_paused = false;
		_time.gtNotify(this);
    }


}
