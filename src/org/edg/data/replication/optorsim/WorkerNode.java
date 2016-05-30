
package org.edg.data.replication.optorsim;

import org.edg.data.replication.optorsim.optor.* ;
import org.edg.data.replication.optorsim.infrastructure.*;
import org.edg.data.replication.optorsim.time.GridTime;

import java.util.*;

import java.lang.Thread;

/**
 * 
 * Worker node class for BatchComputingElement. Doesn't work yet.
 * 
 * Copyright (c) 2005 CERN, ITC-irst, PPARC, on behalf of the EU DataGrid.
 * For license conditions see LICENSE file or
 * <a href="http://www.edg.org/license.html">http://www.edg.org/license.html</a>
 * <p>
 * @author caitrian
 *
 * TODO To change the template for this generated type comment go to
 * Window - Preferences - Java - Code Style - Code Templates
 */

public class WorkerNode implements Runnable {
    private boolean _imAlive ;
    private int _wnID ;
    private String _wnName ;
    private long _totalJobTime = 0;
    private long _workingTime = 0;
    private GridSite _site;
    private BatchComputingElement _ce;
    private JobHandler _jobQueue ;
    private boolean _active=false;
    private float _workerCapacity ;
    private Map _jobTimes = new LinkedHashMap();
    private Map _jobTimesWithQueue = new LinkedHashMap();
    private Map _jobFiles = new LinkedHashMap();
    private long _startRunning ;
    private long _launchTime ;
    private GridTime _time;

    public WorkerNode(BatchComputingElement ce, int wnID, float workerCapacity) {
        _ce = ce ;
        _time = _ce.getGridTime() ;
        _imAlive= true ;
        _wnID = wnID ;
        _wnName = "WN"+_wnID+"@"+_ce ;

        _site = _ce.getSite();
        _jobQueue = _ce.getJobHandler() ;
        _launchTime = _time.getTimeMillis() ; 
        _workerCapacity = workerCapacity ;
        _startRunning = _time.getTimeMillis();	
    }

    public Statistics getStatistics() {

        Map stats = new HashMap();

        float _usage = 100 *_workingTime/(_time.getTimeMillis() - _startRunning);
        stats.put("usage",  new Float(_usage));
        stats.put("jobTimes",  new LinkedHashMap( _jobTimes));
        stats.put("jobTimesWithQueue", new LinkedHashMap(_jobTimesWithQueue));
        stats.put("jobFiles",  new LinkedHashMap(_jobFiles));
        stats.put("totalJobTime",  new Float(_totalJobTime/(float)1000));
        stats.put("totalWorkingTime", new Float(_workingTime/(float)1000));

        return new Statistics(this, stats);
    }

    public String toString() {
        return _wnName ;
    }

    public void run() {
        Thread.currentThread().setPriority(Thread.MAX_PRIORITY);
        OptorSimParameters params = OptorSimParameters.getInstance() ;
        Double execTime;
        
        for(GridJob job=null; (job != null || (!_jobQueue.isEndOfJobs()));) {
        	_active = false ;	    
            job = _jobQueue.get();

            if (job == null){
                continue ;
            }

            job.started();
            long startDate = _time.getTimeMillis() ; 
            _active = true;
 
            // Install our optimiser
            Optimisable replicaOptimiser = OptimiserFactory.getOptimisable( _ce.getSite());

            AccessPatternGenerator accessPatternGenerator 
			= AccessPatternGeneratorFactory.getAPGenerator(job);
            
            String[] _logicalfilenames = new String[1];

            LinkedList filesAccessed  = new LinkedList();
      
            for( String lfn = accessPatternGenerator.getNextFile();
			 lfn != null; 
			 lfn = accessPatternGenerator.getNextFile()) {
            	
            	filesAccessed.add(lfn);

            	// Pack the logical file name into the expected structure:
            	_logicalfilenames[0] = lfn;
            	float[] fileFractions = new float[1];
            	fileFractions[0] = (float)1.0;

            	// Use optimiser to locate best replica of this file
            	DataFile[] files = replicaOptimiser.getBestFile(_logicalfilenames, fileFractions);
            	
            	if( files.length != 1) {
            		OptorSimOut.println( "ASSERT FAILED: CE, getBestFile return array with wrong number of entries: "+  files.length  +" != 1");
            		continue; // skip to next job
            	}	

            	if(files[0] == null) {
            		OptorSimOut.println( _wnName + "> ERROR getBestFile returned"+
                        " null for "+_logicalfilenames[0]);
            		continue; // skip to next job
            	}

            	StorageElement fileSE = files[0].se();
            	GridSite fileSite = fileSE.getGridSite();

//            	 Special case.  If file is remote, then simulate the remoteIO, unPin and move on to next file.
				if( _site != fileSite) {
					_ce.simulateRemoteIO( files[0], fileFractions[0]);

					// log this as an access on the close SE (if it exists!)
					if(_site.hasSEs())
						_site.getCloseSE().accessFile(files[0]);

					execTime = new Double((job.getLatency() + job.getLinearFactor()*files[0].size())/_workerCapacity);
					_time.gtSleep(execTime.longValue());
					
					files[0].releasePin();
					_ce.incRemoteReads();
					continue;
				}
				else {
					fileSE.accessFile(files[0]);
					_ce.incLocalReads();
				}

				// process the file
				execTime = new Double((job.getLatency() + job.getLinearFactor()*files[0].size())/_workerCapacity);
				_time.gtSleep(execTime.longValue());

				files[0].releasePin();
				
		    } // for each datafile in job

            long duration = ( _time.getTimeMillis() - job.timeStarted());
           	long durationWithQueue = (_time.getTimeMillis() - job.timeScheduled());
           	_totalJobTime += durationWithQueue;
            _workingTime += duration;

            if( params.outputStatistics() == 3 || params.useGui()) {
            	_jobTimes.put(job.toString(), new Long(duration));	
            	_jobFiles.put( job.toString(), filesAccessed);
            	_jobTimesWithQueue.put(job.toString(), new Long(durationWithQueue));
            }
            
       } // while there are jobs left to run
     
        _ce.notifyWNShutdown(_wnName);
    } // run

}