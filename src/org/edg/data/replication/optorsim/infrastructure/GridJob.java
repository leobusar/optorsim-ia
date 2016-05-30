package org.edg.data.replication.optorsim.infrastructure;

import org.edg.data.replication.optorsim.time.GridTime;
import org.edg.data.replication.optorsim.time.GridTimeFactory;

import java.util.Random;
import java.util.Vector;

/**
 * A class which defines a job as a vector of DataFiles.
 * <p>
 * Copyright (c) 2002 CERN, ITC-irst, PPARC, on behalf of the EU DataGrid.
 * For license conditions see LICENSE file or
 * <a href="http://www.edg.org/license.html">http://www.edg.org/license.html</a>
 * <p>
 * @since JDK1.4
 */ 
public class GridJob extends Vector{

    private String _name=null;
    private long _jobScheduled;
    private long _jobStarted;
	private double _probability;
	private int _id;
    private GridTime _time = GridTimeFactory.getGridTime();
    private double _latency ;
    private double _linearFactor ;
    private float _fileFraction;
    
    /**
     * Construct a new GridJob with the given name.
     * @param name Name to give this GridJob.
     */
    public GridJob(String name) {
    	_name = name;
    }

	/**
	 * Assign a unique ID to this job. ID is hidden from the outside -
	 * only used in the toString() method.
	 * @param id The ID
	 */
	public void assignID( int id) {
		_id = id;
	}

	/**
	 * Assign a probability for running this job.
	 * @param p The probability
	 */
	public void assignProbability( double p) {
		_probability = p;		
	}
	
	public void setLatency(double latency) {
        _latency = latency ;
    }
	
	public void setLinearFactor(double linearFactor) {
        _linearFactor = linearFactor ;
    }
	
	/** 
	 * */
	public void setFileFraction( float fileFraction){
		 _fileFraction = fileFraction;
	}
	
	public double getLatency(){
        return _latency ;
    }
	
	public double getLinearFactor(){
        return _linearFactor ;
    }
	
	public float getFileFraction() {
		return _fileFraction;
	}
	
	/**
	 * Inquire what the probability of running this job. 
	 * @return Probability
	 */
	public double probability() {
		return _probability;
	}
	
    /**
     * @return The unique name of the instance of this job.
     */
	public String toString() {
		return _name.concat("_"+_id);
    }

    /**
     * Get the name of this GridJob.
     * @return The generic name of this job.
     */
	public String name() {
	    return _name;
    }

	/**
	 * Overload the equals operator.  So cloned jobs are equal
	 */    
    public boolean equals( Object a) {
    	if( ! (a instanceof GridJob) )
    		return false;
    	GridJob j = (GridJob) a;
    	return ( j._name == _name);    	
    }

    /**
     * Clone this GridJob.
     * @return the cloned job.
     */
	public GridJob cloneJob() {
		return (GridJob)super.clone();
	}
	
    /**
     * Record the time a GridJob was scheduled.
     */
    public void scheduled() {
    	_jobScheduled = _time.getTimeMillis();
    }

    /**
     * Record the time a GridJob was started.
     */
    public void started() {
    	_jobStarted = _time.getTimeMillis();
    }

    /**
     * Query when this job was scheduled
     * @return The time this job was scheduled, or null if it hasn't
     * been scheduled yet. 
     */
    public long timeScheduled() {
    	return _jobScheduled;
    }
    
    /**
     * Query when this job was started.
     * @return The time this job was started, or null if it hasn't
     * been started yet.
     */
    public long timeStarted() {
    	return _jobStarted;
    }
    
    /**
     * CN: Cuts the files required by the job from the whole 
     * fileset to the number required by the fileFraction, starting
     * at a random point.
     */
    public void trimJob() {
    	Random random;
    	int numFiles;
    	int start;
    	boolean randomSeed = OptorSimParameters.getInstance().useRandomSeed();
    	float startValue;
    	if(randomSeed) {
			random = new Random();
			startValue = random.nextFloat();
		}
		else {
			random = new Random(99);
			startValue = (new Random(98)).nextFloat();
		}
    	numFiles = (int)(this.size() * _fileFraction);
    	start = (int)(startValue*this.size());
    	if (size() - numFiles <= start) {
    		this.removeRange(numFiles+start-size(), start);		
    	}
    	else {
    		this.removeRange(0,start);
    		this.removeRange(numFiles,indexOf(this.lastElement()));
    		this.remove(this.lastElement());
    	}
    }
}
