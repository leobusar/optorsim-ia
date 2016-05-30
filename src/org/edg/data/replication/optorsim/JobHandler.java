package org.edg.data.replication.optorsim;

import java.util.LinkedList;
import java.util.List;
import java.util.Iterator;
import java.util.Map;
import java.util.HashMap;

import org.edg.data.replication.optorsim.infrastructure.ComputingElement;
//import org.edg.data.replication.optorsim.infrastructure.DataFile;
import org.edg.data.replication.optorsim.infrastructure.GridJob;
//import org.edg.data.replication.optorsim.reptorsim.ReplicaManager;
import org.edg.data.replication.optorsim.time.GridTimeFactory;
import org.edg.data.replication.optorsim.time.GridTime;
import org.edg.data.replication.optorsim.optor.Optimisable;

/**
 * This class controls the flow of jobs into a 
 * {@link org.edg.data.replication.optorsim.infrastructure.ComputingElement}
 * using a queueing system. It provides the methods for communication
 * between the ResourceBroker and ComputingElement threads.
 * <p>
 * Copyright (c) 2002 CERN, ITC-irst, PPARC, on behalf of the EU DataGrid.
 * For license conditions see LICENSE file or
 * <a href="http://www.edg.org/license.html">http://www.edg.org/license.html</a>
 * <p>
 * @since JDK1.4
 */ 
public class JobHandler {

    private int _queueLimit;
    private int _jobCount=0;
    private LinkedList _jobQueue = new LinkedList();
    private List _runningJobQueue = new LinkedList();
    private boolean _endOfJobs = false;
	private GridTime _time = GridTimeFactory.getGridTime();

    public JobHandler(int queueLimit) {
		_queueLimit = queueLimit;
    }

    /**
     * The method called by the ComputingElement to obtain a job
     * from the resource broker. If the job queue is empty
     * the ComputingElement thread waits until the ResourceBroker
     * calls put().
     *
     * @return The first GridJob in the queue.
     */
    public GridJob get() {
    	GridJob nextJob;
        boolean jobQEmpty;

		synchronized( _jobQueue) {
			jobQEmpty = _jobQueue.isEmpty();
        }

        if (jobQEmpty && !_endOfJobs) {
    //        System.out.println(this.toString()+"> waiting for jobs...");
            _time.gtWait(this);
        }

        synchronized( _jobQueue) {
			if (_jobQueue.isEmpty()) {
                if(! _endOfJobs)
                    System.out.println( "JobHandler> BUG received notify() on an empty jobQueue without endOfJobs set");
                else{
                	_time.gtNotify(this);
                }
                return null;				
			}

			_jobCount++;
			nextJob = (GridJob)_jobQueue.get(0);
			_jobQueue.remove(0);
		}

		return nextJob;
    }
    

    /**
     * The method called by the ResourceBroker to give a job to
     * the ComputingElement. The job is added to the end of the
     * job queue unless the queue is full.
     *
     * @return true if job was successfully submitted, false if
     * the job queue is full.
     */
    public boolean put(GridJob gridJob) {

		// this number puts a limit on the size of the queue
		synchronized( _jobQueue) {
			if (_jobQueue.size() >= _queueLimit)
				return false;
            _jobQueue.add( gridJob);
            //System.out.println(this.toString()+"> got a job now");
        }
        _time.gtNotify(this);

        return true;
    }

    /**
     * Method called by the ResourceBroker at the end of the
     * simulation to interrupt waiting ComputingElements so
     * their threads will end.
     */
    public void interruptJobHandler() {
        _endOfJobs = true;
        _time.gtNotify(this);
    }

    /**
     * @return The current queue size.
     */
    public int getQueueSize() {
    	synchronized( _jobQueue) {
			return _jobQueue.size();
    	}
    }

    /**
     * @return True if queue is full, false if not.
     */
    public boolean isFull() {
    	synchronized( _jobQueue) {
			if (_jobQueue.size() >= _queueLimit)
				return true;
			else
				return false;
    	}
    }

    /**
     * Calculate the cost of running all the jobs queued at 
     * the given ComputingElement. Some optimisation is performed
     * by storing the cost of each job, then if the same job appears
     * again in the queue the stored cost is used instead of calculating
     * it again. This assumes the state of the Grid does not change
     * during this method.
     */
	public double getQueueAccessCost( Optimisable optor, ComputingElement ce) {
		double totalCost = 0;
		Map jobCosts = new HashMap();

		synchronized(_jobQueue) {
			for (Iterator iJobs = _jobQueue.iterator(); iJobs.hasNext(); ) {
			
				GridJob job = (GridJob)iJobs.next();
				String[] lfns = new String[job.size()];
				float[] fileFractions = new float[job.size()];
	
				job.toArray( lfns);
				for( int i=0; i < job.size(); i++)
				fileFractions [i] = (float)1.0;

				if (jobCosts.containsKey(job.name()))
				    totalCost += ((Double)jobCosts.get(job.name())).doubleValue();

				else {
				    double cost = optor.getAccessCost(lfns, ce, fileFractions);
				    totalCost += cost;
				    jobCosts.put(job.name(), new Double(cost));
				}
//				AV
  /*              double estimatedDuration = job.getLatency();
                int workerNodes = ce.getWorkerNodes();
                DataFile[] dataFiles = null ;
                ReplicaManager rm = ReplicaManager.getInstance();

                for(int j=0; j <job.size(); j++){
                    dataFiles = rm.listReplicas((String)job.get(j)) ;
                    estimatedDuration += job.getLinearFactor()*(dataFiles[0].size());
                }

                estimatedDuration /= workerNodes ; 
                totalCost += estimatedDuration ; 
                //end AV
	*/			
			}
		}
		return totalCost;
    }

	/**
	 * Find out the job count
	 * @return number of jobs that have either been processed or are being processed.
	 */
	public int jobCount() {
		return _jobCount;
	}

	/**
     * The method called by the WorkerNode to reinsert a job to
     * the ComputingElement. The job is added to the begin of the
     * job queue unless the queue is full.
     *
     * @return true if job was successfully submitted, false if
     * the job queue is full.
     */
    public boolean putFirst(GridJob gridJob) {

        // this number puts a limit on the size of the queue
        synchronized( _jobQueue) {
            if (_jobQueue.size() >= _queueLimit)
                return false;
            else {
                _jobQueue.addFirst( gridJob);
                _time.gtNotify(this);
                //                _jobQueue.notify();
                return true;
            }
        }
    }
	
	/**
     * @return false if there are still job that can come, otherwise true
     */
    public boolean isEndOfJobs() {
        return _endOfJobs ;
    }
	
}
