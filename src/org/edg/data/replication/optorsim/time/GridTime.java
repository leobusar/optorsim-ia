package org.edg.data.replication.optorsim.time;

import java.util.Date;

/**
 * GridTime provides other classes with an abstract idea of time, sometimes called
 * Simulation Time.  It is monotonic increasing, but other than that no guarantees are provided
 * about the relationship between this measure of time and "real" time.
 * 
 *  Copyright (c) 2004 CERN, ITC-irst, PPARC, on behalf of the EU DataGrid.
 * For license conditions see LICENSE file or
 * <a href="http://www.edg.org/license.html">http://www.edg.org/license.html</a>
 * <p>
 */
public interface GridTime {

	/** Get the current simulation time in milliseconds.*/
    long getTimeMillis();

    /** Get the current simulation time as a Date object.*/
    Date getDate();

    /** Get the current time as a time of day, to the nearest half hour.*/
    float roundedTimeOfDay();

    /** Get the time as a string. */
	String toString();
	
	/** Stop simulation time.*/
	void stop();
	
	/** Start simulation time.*/
	void start();
	
	/** Implementation of sleep() according to the time model used. */
	void gtSleep(long delay);

	/** Implementation of wait() according to the time model used. */
	void gtWait(Object waitObject);
	
	/** Implementation of wait(long delay) according to the time model used. */
	void gtWait(Object waitObject, long delay);
	
	/** Implementation of notify() according to the time model used. */
	void gtNotify(Object waitObject);
	
	/** Implementation of Thread.join() according to the time model used. */
	void gtJoin(Thread t);

    /**
     * current hour
     */
	int getHours();

    /**
     * current minute
     */
	int getMinutes();

    /**
     *  current second
     */
	int getSeconds();

    /**
     * Simulation time in ms since the start of simulation.
     */
    long getRunningTimeMillis();

}
