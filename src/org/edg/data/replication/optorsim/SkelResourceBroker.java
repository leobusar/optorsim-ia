package org.edg.data.replication.optorsim;

import org.edg.data.replication.optorsim.auctions.AuctionThreadPool;
import org.edg.data.replication.optorsim.infrastructure.ComputingElement;
import org.edg.data.replication.optorsim.infrastructure.GridContainer;
import org.edg.data.replication.optorsim.infrastructure.GridJob;
import org.edg.data.replication.optorsim.infrastructure.OptorSimParameters;
import org.edg.data.replication.optorsim.optor.Optimisable;
import org.edg.data.replication.optorsim.optor.OptimiserFactory;
import org.edg.data.replication.optorsim.time.GridTime;
import org.edg.data.replication.optorsim.time.GridTimeFactory;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * Provides all of the functionality required by all the Resource Brokers
 * apart from the findCE() method which must be defined by each subclass.
 * This method returns the Computing Element to which the job must be
 * submitted, according to the scheduling algorithm.
 * <p>
 * Copyright (c) 2004 CERN, ITC-irst, PPARC, on behalf of the EU DataGrid.
 * For license conditions see LICENSE file or
 * <a href="http://www.edg.org/license.html">http://www.edg.org/license.html</a>
 * <p>
 * @since JDK1.4
 */
abstract public class SkelResourceBroker implements ResourceBroker {

    private boolean _paused = false;
    private OptorSimParameters _params;
    private GridTime _time;
    private List _jobQueue = new LinkedList();
    private boolean _iAmAlive;

    /**
     * This constructor should only be called from SkelResourceBroker
     * subclasses.
     */
    protected SkelResourceBroker() {

        // Boost our priority
        Thread.currentThread().setPriority(Thread.MAX_PRIORITY);

		_params = OptorSimParameters.getInstance();
        _time = GridTimeFactory.getGridTime();
		System.out.println( "ResourceBroker> Starting up ...");
        _iAmAlive = true;
    }
    
    /**
     * Returns the Computing Element to which the job must be
     * submitted, according to the scheduling algorithm implemented
     * by the subclass.
     */
    abstract protected ComputingElement findCE( GridJob job, Optimisable optor);

    /**
     * The ResourceBroker maintains a queue of jobs submitted by Users
     * which it processes until the queue is empty, then waits until
     * more are submitted.
     */
    public void run() {

        System.out.println("Simulation starting "+_time.getDate());

        // If we've been told to wait, do so:
		checkIfPaused();

        List jobsToProcess = new LinkedList();

        while (_iAmAlive || !_jobQueue.isEmpty()) {

            standBy();

            // take all the jobs in the queue and process them outside the
            // synchronisation block so we don't block the users submitting more
            synchronized( _jobQueue) {
                if (!_jobQueue.isEmpty()) {
                    jobsToProcess.addAll( _jobQueue);
                    _jobQueue.clear();
                }
            }

            for( Iterator i = jobsToProcess.iterator(); i.hasNext();) {
                GridJob job = (GridJob) i.next();
                ComputingElement ce;

                for(;;) {
      //          	System.out.println("ResourceBroker> looking for free CE...");
                    ce = findCE( job, OptimiserFactory.getOptimisable());

                    if( ce != null)
                    break;

                    // if we can't schedule to any CE wait and try again
                    // TODO: find a better way to do this. Would rather wait
                    // until a CE becomes free.
    //                System.out.println(this.toString()+"> waiting for a free CE...");
                    _time.gtSleep( _params.getJobDelay());
                }
                OptorSimOut.println("ResourceBroker> Submitting job "+job+
	                                 " to " + ce);
                // submit the job
                ce.getJobHandler().put( job);
                checkIfPaused();
            }

            jobsToProcess.clear();
        }
        endSimulation();

		System.out.println( "ResourceBroker> Shutting myself down now (farewell, cruel world)");
        System.out.println("Simulation ended "+_time.getDate());
    }

    /**
     * Wait until a job is submitted
     */
    private synchronized void standBy() {
        synchronized( _jobQueue) {
            // If there are jobs to process, return immediately.
            if( _jobQueue.size() != 0)
                return;
        }
        _time.gtWait(this);
    }

    /**
     * Tell the RB that it can shut itself down after it has processed
     * any jobs that are left.
     */
    public synchronized void shutDownRB() {
		_iAmAlive = false;
        _time.gtNotify(this);
    }

    /**
     * Submit a job to the RB which will decide which CE to pass
     * it on to.
     * @param job The job a user wishes to run.
     */
    public synchronized void submitJob(GridJob job) {
		synchronized( _jobQueue) {
			_jobQueue.add(job);
        }
		_time.gtNotify(this);
    }

    /**
     * Method that checks if RB is paused and if it is, waits until the RB is unpaused.
     */
    private void checkIfPaused() {
		while(_paused) {
            _time.gtWait(this);
        }
    }
    
    /**
     * Stops the ResourceBroker from submitting any jobs until the
     * unPauseRB() method is called.
     */
    public void pauseRB() {
		_paused = true;
    }

    /**
     * Starts the ResourceBroker going again after it has been paused.
     */
    public void unPauseRB() {
		_paused = false;
		_time.gtNotify(this);
    }

    /**
     * The RB is responsible for shutting down all the Grid elements
     * at the end of the simulation. Once this is done it prints out
     * the required level of statistics.
     */
    private void endSimulation() {

        GridContainer gc = GridContainer.getInstance();

        //  Shut down all CEs.  This will block until all CE threads are
        //  finished, ie after all jobs are done.
        gc.shutDownAllCEs();

        // When we get to here, all CEs have been shut down. No jobs are running.
        System.out.println( "ResourceBroker> all jobs finished, shutting down P2P network ...");
        gc.shutDownAllP2P();

        // now shut down all the auctions
        AuctionThreadPool.killAllAuctions();

        // print out useful information before shutting down
        switch (_params.outputStatistics()) {
            case FULL_STATS:
           //     gc.printState();
                gc.getStatistics().recursivePrintStatistics();
                break;
            case SIMPLE_STATS:
                gc.getStatistics().printStatistics();
                break;
            case NO_STATS:
                break;
            default:
                System.out.println(" WARNING: Unknown statistics level: "
                        +_params.outputStatistics());
        }

    }

    private static final int FULL_STATS = 3;
    private static final int SIMPLE_STATS = 2;
    private static final int NO_STATS = 1;

}
