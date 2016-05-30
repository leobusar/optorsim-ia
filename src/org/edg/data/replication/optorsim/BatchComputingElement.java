package org.edg.data.replication.optorsim;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Vector;

import org.edg.data.replication.optorsim.infrastructure.ComputingElement;
import org.edg.data.replication.optorsim.infrastructure.GridSite;
import org.edg.data.replication.optorsim.infrastructure.OptorSimParameters;
import org.edg.data.replication.optorsim.infrastructure.Statistics;
import org.edg.data.replication.optorsim.time.GridTime;
import org.edg.data.replication.optorsim.time.GridTimeFactory;

/**
 * @author caitrian
 *
 * Copyright (c) 2002 CERN, ITC-irst, PPARC, on behalf of the EU DataGrid.
 * For license conditions see LICENSE file or
 * <a href="http://www.edg.org/license.html">http://www.edg.org/license.html</a>
 * <p>
 * TODO To change the template for this generated type comment go to
 * Window - Preferences - Java - Code Style - Code Templates
 */

class WNShutdown {

    private int _shutdownWN ;
    private int _workerNodes ;
    private GridTime _time = GridTimeFactory.getGridTime();

    public WNShutdown( int workerNodes) {
        _shutdownWN = 0 ;
        _workerNodes = workerNodes ;
    }

    public synchronized void notifyWNShutdown()
    {
        _shutdownWN++;
        _time.gtNotify(this);
    }

    public synchronized void waitShutdown() {
        while (_shutdownWN < _workerNodes) {
           _time.gtWait(this);
       }
    }
}


public class BatchComputingElement extends SimpleComputingElement implements
		ComputingElement {

    private Vector _wnThreads = new Vector();
    private WNShutdown _wnShutdown ;
	
	/**
	 * @param site
	 * @param workerNodes
	 */
	public BatchComputingElement(GridSite site, int workerNodes, float capacity) {
		super(site, workerNodes, capacity);
		_wnShutdown = new WNShutdown(workerNodes) ;
	}

	/**
     * A method to return the GridTime of this CE.
     */
    public GridTime getGridTime() {
        return _time;
    }
	
    public int getWorkerNodes() {
        return _workerNodes;
    }

    public void notifyWNShutdown(String wnName){
    	_wnShutdown.notifyWNShutdown();
    }
    
    public void run() {
        Thread.currentThread().setPriority(Thread.MAX_PRIORITY);
        OptorSimParameters params = OptorSimParameters.getInstance() ;
        
        _runnable = true ;
        
        for(int i=0;i<_workerNodes;i++)
        {  
            WorkerNode wn = new WorkerNode( this , i, _workerCapacity);
            Thread wnThread = new Thread(GridTimeFactory.getThreadGroup(), wn );
            wnThread.start();
            _wnThreads.add(wn);
        }
 
        _wnShutdown.waitShutdown();

    } // run

	/**
	 * Method to collate and return information relevant 
	 * to this CE as a {@link Statistics} object.
	 * @return The statistics of this CE
	 */
	public Statistics getStatistics() {
		Map stats = new HashMap();
		float totalJobTime = 0;
		float workingTime = 0;
	    float usage = 0;
	    LinkedHashMap jobTimes = new LinkedHashMap();
	    LinkedHashMap jobTimesWithQueue = new LinkedHashMap();
	    LinkedHashMap jobFiles = new LinkedHashMap();
	    
		for (int i=0; i < _workerNodes; i++){
			WorkerNode wn = (WorkerNode)_wnThreads.elementAt(i);
			Statistics wnStats = wn.getStatistics();
			float wnUsage = wnStats.getFloatStatistic("usage");
			LinkedHashMap wnJobTimes = (LinkedHashMap)wnStats.getStatistic("jobTimes");
			LinkedHashMap wnJobTimesWithQueue = (LinkedHashMap)wnStats.getStatistic("jobTimesWithQueue");
			LinkedHashMap wnJobFiles = (LinkedHashMap)wnStats.getStatistic("jobFiles");
			float wnTotalJobTime = wnStats.getFloatStatistic("totalJobTime");
			float wnWorkingTime = wnStats.getFloatStatistic("totalWorkingTime");
			
			usage += wnUsage;
			totalJobTime += wnTotalJobTime;
			workingTime += wnWorkingTime;
			jobTimes.putAll(wnJobTimes);
			jobTimesWithQueue.putAll(wnJobTimesWithQueue);
			jobFiles.putAll(wnJobFiles);
		}
		
		int jobsCompleted = _inputJobHandler.jobCount();
		stats.put("usage", new Float(usage));
		stats.put("totalJobTime", new Float(totalJobTime));
		float meanJobTime = 0;
		if (jobsCompleted != 0)
		   meanJobTime = workingTime/jobsCompleted;
		stats.put("meanJobTime", new Integer(new Float(meanJobTime).intValue()));
		stats.put("remoteReads", new Long(_remoteReads));
		stats.put("localReads", new Long(_localReads));
		stats.put("jobTimes", jobTimes);
		stats.put("jobTimesWithQueue", jobTimesWithQueue);
		stats.put("jobFiles", jobFiles);
		stats.put("workerNodes",  new Integer(_workerNodes));
		stats.put("status",  new Boolean(_active));
		stats.put("queueLength",  new Integer(_inputJobHandler.getQueueSize()));
		stats.put("numberOfJobs", new Integer(_inputJobHandler.jobCount()));
		stats.put("runnableStatus",  new Boolean(_runnable));
	
		return new Statistics(this, stats);
	}
	
    protected synchronized void incLocalReads() {
        _localReads++;
    }
    
    protected synchronized void incRemoteReads() {
        _remoteReads++;
    }
}
