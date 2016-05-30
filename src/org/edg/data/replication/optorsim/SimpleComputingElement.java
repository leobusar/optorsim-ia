package org.edg.data.replication.optorsim;

import org.edg.data.replication.optorsim.optor.*;
import org.edg.data.replication.optorsim.infrastructure.*;
import org.edg.data.replication.optorsim.time.GridTime;
import org.edg.data.replication.optorsim.time.GridTimeFactory;

import java.util.*;

/**
 * The ComputingElement runs a thread which executes the GridJobs
 * given to it through its {@link JobHandler}. For each file required,
 * the ComputingElement calls <em>getBestFile()</em>, which returns
 * the location of the best replica of the file according to the
 * chosen optimisation algorithm, which may or may not have performed
 * replication. The ComputingElement reads the file from this location
 * and processes it. The time to process the file is calculated as the
 * time specified in the parameters file divided by the number of worker
 * nodes in the ComputingElement.
 * <p>
 * Each ComputingElement can currently run only one job at a time.
 * Information on the time taken for each job can be found in the 
 * statistics output at the end of the simulation if statistics level 3
 * is selected in the parameters file, or from the job time histograms
 * if the GUI is used.
 * <p>
 * Copyright (c) 2002 CERN, ITC-irst, PPARC, on behalf of the EU DataGrid.
 * For license conditions see LICENSE file or
 * <a href="http://www.edg.org/license.html">http://www.edg.org/license.html</a>
 * <p>
 * @since JDK1.4
 */
public class SimpleComputingElement implements ComputingElement {

	private static int _LastCEId = 0;

	private GridSite _site;	
    private String _ceName;  
    private boolean _imAlive;
    private boolean _paused = false;
	private int _CEId;
	private long _workingTime = 0;
    private long _startRunning;
	private long _totalJobTime = 0;
	private Map _jobTimes = new LinkedHashMap();
	private Map _jobTimesWithQueue = new LinkedHashMap();
	private Map _jobFiles = new LinkedHashMap();
	private int _jobsCompleted = 0;
	
	protected JobHandler _inputJobHandler;
	protected boolean _runnable = false;
	protected boolean _active=false;
	protected long _remoteReads = 0;
	protected long _localReads = 0;
	protected int _workerNodes = 0;
	protected float _workerCapacity = 0;
	protected GridTime _time;
	
    public SimpleComputingElement( GridSite site,  int workerNodes, float capacity) {
					
		OptorSimParameters params = OptorSimParameters.getInstance();

        _time = GridTimeFactory.getGridTime();
		_site = site;
		_workerNodes = workerNodes;
		_workerCapacity = capacity;
		_CEId = ++_LastCEId;
		_ceName = "CE"+_CEId+"@"+_site;
		_inputJobHandler = new JobHandler( params.getMaxQueueSize());
		_imAlive = true;
		_site.registerCE( this);
        _startRunning = _time.getTimeMillis();
    }
    
    /**
     * Return a more meaningful name.
     * @return the CE's name
     */
    public String toString() {
    	return _ceName;
    }
    
    /**
     * Check whether this CE is active (processing jobs) or idle.
     */
	public boolean active() {
		return _active;
	}

	/**
	 * Check whether this CE is still running or has been shut down.
	 */
	public boolean imAlive() {
		  return _imAlive;
	}

    /**
     * A method to return the input sandbox for this computing element.
     */
    public JobHandler getJobHandler() {
		return _inputJobHandler;
    }

    /**
     * Method to get the site that this CE is on.
     * @return The site this CE is on.
     */
    public GridSite getSite() {
		return _site;
    }

		
    /**
     * Method to give the name of this CE.
     * @return The name of this CE.
     */
    public String getCeName() {
		return _ceName;
    }

    public int getWorkerNodes() {
        return _workerNodes;
    }
    
    /**
     * Method to check against our ID 
     */
    public boolean iAm( int id) {
    	return _CEId == id;
    }
	
	/**
	 * Method to collate and return information relevant 
	 * to this CE as a {@link Statistics} object.
	 * @return The statistics of this CE
	 */
	public Statistics getStatistics() {
		Map stats = new HashMap();
		OptorSimParameters params = OptorSimParameters.getInstance();
		float _usage = _time.getTimeMillis() - _startRunning == 0 ? 0 : 100 *_workingTime/(_time.getTimeMillis() - _startRunning);
		stats.put("usage",  new Float(_usage));
		stats.put("remoteReads",  new Long(_remoteReads));
		stats.put("localReads",  new Long(_localReads));
		if( params.outputStatistics() ==3) {
			stats.put("jobTimes",  new LinkedHashMap( _jobTimes));
			stats.put("jobTimesWithQueue", new LinkedHashMap(_jobTimesWithQueue));
			stats.put("jobFiles",  new LinkedHashMap(_jobFiles));
			stats.put("numberOfJobs", new Integer(_jobsCompleted));
			stats.put("workerNodes",  new Integer(_workerNodes));
			stats.put("status",  new Boolean(_active));
			stats.put("queueLength",  new Integer(_inputJobHandler.getQueueSize()));
			stats.put("runnableStatus",  new Boolean(_runnable));
		}
		stats.put("totalJobTime",  new Float(_totalJobTime/(float)1000));
		
		long meanJobTime = 0;
		if (_jobsCompleted!=0)
		   meanJobTime = _workingTime/_jobsCompleted;
		  /////////////////////////////////////////
		stats.put("meanJobTime",  new Long(meanJobTime));
		
		return new Statistics(this, stats);
	}

    /**
     * When running, the ComputingElement processes all the jobs
     * submitted to it through the JobHandler, sleeping while the
     * JobHandler is empty. It is notified to shut down by the 
     * ResourceBroker.
     */
    public void run() {

        // Boost our priority
        Thread.currentThread().setPriority(Thread.MAX_PRIORITY);
        Double execTime;
		OptorSimParameters params = OptorSimParameters.getInstance();

    	_runnable = true;
		
		// to keep thread running
		for( GridJob job=null; job != null || _imAlive; ) {
				
		    _active=false;
		    job=_inputJobHandler.get();  // This potentially blocks
		    
		    // We might get a null job from JobHandler, if so, skip any further activity
		    if( job == null)
			continue;
				
		    job.started();
	
		    OptorSimOut.println(_ceName+"> starting to process "+job+" (queue length now "+
				       _inputJobHandler.getQueueSize()+")");
		    _active=true;	    

		    // Install our optimiser
		    Optimisable replicaOptimiser = OptimiserFactory.getOptimisable( _site);

		    AccessPatternGenerator accessPatternGenerator 
			= AccessPatternGeneratorFactory.getAPGenerator(job);
			
		    String[] logicalfilenames = new String[1];
					
		    List filesAccessed  = new LinkedList();		
					
		    for( String lfn = accessPatternGenerator.getNextFile();
			 lfn != null; 
			 lfn = accessPatternGenerator.getNextFile()) {
			
				filesAccessed.add(lfn);
				
				// Pack the logical file name into the expected structure:		
				logicalfilenames[0] = lfn;
				float[] fileFractions = new float[1];
				fileFractions[0] = (float)1.0;

					// Use optimiser to locate best replica of this file
				DataFile[] files = replicaOptimiser.getBestFile(logicalfilenames, 
									     		fileFractions);
				if( files.length != 1) {
					System.out.println( "ASSERT FAILED: CE, getBestFile return array with wrong number of entries: "+  files.length  +" != 1");
					continue; // skip to next file
				}

				if(files[0] == null) {
					System.out.println( _ceName + "> ERROR getBestFile returned"+
						" null for "+logicalfilenames[0]);
					continue; // skip to next file
				}

				StorageElement fileSE = files[0].se();
				GridSite fileSite = fileSE.getGridSite();

				// Special case.  If file is remote, then simulate the remoteIO, unPin and move on to next file.
				if( _site != fileSite) {
					simulateRemoteIO( files[0], fileFractions[0]);

					// log this as an access on the close SE (if it exists!)
					if(_site.hasSEs())
						_site.getCloseSE().accessFile(files[0]);

					if(_workerNodes != 0) {
						execTime = new Double((job.getLatency() + job.getLinearFactor()*files[0].size())/(_workerNodes*_workerCapacity));
						_time.gtSleep(execTime.longValue());
					}
					files[0].releasePin();
					_remoteReads++;
					continue;
				}
				else {
					fileSE.accessFile(files[0]);
					_localReads++;
				}

				// process the file
				if(_workerNodes != 0) {
					execTime = new Double((job.getLatency() + job.getLinearFactor()*files[0].size())/(_workerNodes*_workerCapacity));
	//				System.out.println(this.toString()+"> processing file...");
					_time.gtSleep(execTime.longValue());
				}
				
				files[0].releasePin();
										
				//A while loop the ce enters when paused by gui
                while(_paused){
                    _time.gtWait(this);
                }

		    } // for each datafile in job

		    // statistics logging
		    long duration = _time.getTimeMillis() - job.timeStarted();
		    long durationWithQueue = _time.getTimeMillis() - job.timeScheduled();
            if( duration < 0) {
                OptorSimOut.println("BUG> Duration < 0!!");
            }
		    _totalJobTime += durationWithQueue;
		    _workingTime += duration;
			_jobsCompleted++;
			
			if( params.outputStatistics() == 3 || params.useGui()) {
				_jobTimes.put(job.toString(), new Long(duration));
				_jobTimesWithQueue.put(job.toString(), new Long(durationWithQueue));
				_jobFiles.put( job.toString(), filesAccessed);
			}
			
		} // while there are jobs left to run	     
		_runnable = false;
    } // run


    /**
     * A routine used by the CE to simulate remote IO. The GridContainer's copy() method is
     * used to block the equivalent amount of time.
     */
    protected void simulateRemoteIO( DataFile remoteFile, float fraction) 
    {
		GridContainer gc = GridContainer.getInstance();
		gc.copy( remoteFile, _site, fraction);
    }


    /**
     * GUI calls this method to pause the ComputingElement
     * threads when pause button is pressed.
     */
    public void pauseCE() {
		_paused = true;
    }

    /**
     * GUI calls this method to unpause the ComputingElement
     * threads when continue button is pressed.
     */
    public void unpauseCE() {
		_paused = false;
		_time.gtNotify(this);
    }

    /**
     * The ResourceBroker calls this method when it has
     * distributed all the jobs to shut down the ComputingElement
     * threads.
     */
    public void shutDownCE(){
		_imAlive = false;
    }
}
