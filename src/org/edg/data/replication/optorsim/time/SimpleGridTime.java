package org.edg.data.replication.optorsim.time;

import org.edg.data.replication.optorsim.infrastructure.OptorSimParameters;

import java.util.Calendar;
import java.util.Date;

/**
 * In SimpleGridTime simulation time is the same as real time. The
 * wall clock time to complete a simulation run is the same as
 * the time to complete all the jobs in simulation time. Everything
 * therefore done in the simulation which takes any time counts towards
 * simulation time.
 * <p>
 * Simulation time is represented by a Long, representing the
 * number of ms in Grid time since the sim started. The class is a
 * singleton, with the {@link #getInstance()} method returning a
 * pointer to the single instance of the GridTime object.
 * <p>
 * Copyright (c) 2004 CERN, ITC-irst, PPARC, on behalf of the EU DataGrid.
 * For license conditions see LICENSE file or
 * <a href="http://www.edg.org/license.html">http://www.edg.org/license.html</a>
 * <p>
 *
 */

public class SimpleGridTime implements GridTime{
	
	/**
	 * STATIC 
	 */
	private static SimpleGridTime _instance=null;
    private long _realTimeSimulationStart;

    /** Returns a (singleton) instance of the GridTime.*/
    public static GridTime getInstance() {
		if( _instance == null)
			_instance = new SimpleGridTime();
		return _instance;		
	}
	
	/**
	 * INSTANCE VARIABLES and METHODS
	 */
	
	private Calendar _cal;
	/** The simulation time, in milliseconds, at which the simulation was started.*/
	protected long _simulationStartTime;
    private long _timeStoppedAt, _timeStoppedFor;
	private boolean _timeStopped = false;
	
	
	/**
	 * Private constructor for our singleton class. Constructs a Calendar
     * to represent the starting time of the simulation (today at the time
     * specified in the parameters file). If this fails, we fall back
     * to the current time.
	 */
	protected SimpleGridTime() {

        _cal = Calendar.getInstance();
        _cal.setLenient(false);
		OptorSimParameters params = OptorSimParameters.getInstance();
		float p = params.startTimeOfDay();
 		int hour24 = Math.round( (float) Math.floor( p));
        int hour12 = 0;
		int newMinute = Math.round( MINUTES_IN_HOUR * (p - hour24));
	
		try {
            if (hour24 < 12) {
                _cal.set( Calendar.AM_PM, Calendar.AM);
                hour12 = hour24;
			 }
            else {
				_cal.set( Calendar.AM_PM, Calendar.PM);
                hour12 = hour24 - 12;
            }
            _cal.set( Calendar.HOUR_OF_DAY, hour24);
			_cal.set( Calendar.HOUR, hour12);
			_cal.set( Calendar.MINUTE, newMinute);
            _cal.set( Calendar.SECOND, 0);
			//check arguments are OK
			_cal.getTimeInMillis();
		} catch ( IllegalArgumentException e) {
			System.out.println( "The initial Hour Of Day value of "+params.startTimeOfDay()+" is invalid.");
			_cal = Calendar.getInstance();
		}

		_simulationStartTime = _cal.getTimeInMillis();
        _realTimeSimulationStart = System.currentTimeMillis();
	}


	/**
	 * Private method that resynchronises our Calendar object with the current
	 * simulation time.
	 */
	private void updateCal() {
		_cal.setTimeInMillis( getTimeMillis());
	}


	/**
	 * Stop time. All future calls to query the time will produce the same results
	 * unless time is started again.
	 */
	public void stop() {
		if( _timeStopped)
			return;

		 // We note the (Real) time we stopped simulation time and the fact that
		 // time is stopped.  When we start time up again, this is taken into account.
		_timeStopped = true;
		_timeStoppedAt = System.currentTimeMillis();
	}
	
	/**
	 * Start time ticking again.  The result of querying the time immediately before and
	 * after calling start() should be the same.
	 */
	public void start() {
		if( ! _timeStopped)
			return;
		
        // To have the effect of time having been paused, we advance the start times accordingly.
		_timeStoppedFor += System.currentTimeMillis() - _timeStoppedAt;
        _timeStopped = false;
	}
	
	/**
	 * Gives a String representation of simulation time.
     * @return Simulation time in a friendly format.
	 */
	public String toString() {
        return getDate().toString();
	}

    /**
     * Gives the time in ms (simulation time).
     * @return Simulation time in ms.
     */
	public long getTimeMillis() {
        // ms of simulation time since start
        long simTime = (long)((System.currentTimeMillis() - _realTimeSimulationStart - _timeStoppedFor));
		return simTime + _simulationStartTime;
	}

    /**
     * The Date object representing current simulation time.
     */
    public Date getDate() {
        return new Date(getTimeMillis());
    }

    public float roundedTimeOfDay() {

        updateCal();
        float hour = _cal.get( Calendar.HOUR_OF_DAY);
        hour += _cal.get( Calendar.MINUTE) / (float) MINUTES_IN_HOUR;
        hour += _cal.get( Calendar.SECOND) / (float) SECONDS_IN_HOUR;
        return hour;
    }


	/**
	 * Puts the current thead to sleep for the specified number of milliseconds
	 * @param delay the time, in milliseconds, for which the current thread should be put asleep.
	 */
	public void gtSleep( long delay) {
		try {
			Thread.sleep( delay);
		} catch ( InterruptedException e) {
			System.out.println( "Thread "+Thread.currentThread()+" was interrupted whilst sleeping...");
		}
	}
 
	/** Sets the current thread to wait until the waitToken object
	 * is notified.
	 */
    public void gtWait(Object waitToken) {
        synchronized (waitToken) {
            try {
                waitToken.wait();
            } catch( InterruptedException e) {
               e.printStackTrace();
            }
        }
    }

    /** Sets the current thread to wait until the waitToken object
	 * is notified or <i> time</i> has passed.
	 */
    public void gtWait(Object waitToken, long time) {
        synchronized (waitToken) {
            try {
                waitToken.wait(time);
            } catch( InterruptedException e) {
               e.printStackTrace();
            }
        }
    }

    /** Notifies the thread on the waitToken object.*/
    public void gtNotify(Object waitToken) {
        synchronized (waitToken) {
            waitToken.notify();
        }
    }

    /** Waits for the thread to die.*/
    public void gtJoin(Thread t) {
        try {
            t.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    final int MINUTES_IN_HOUR = 60;
	final int SECONDS_IN_HOUR = 3600;

    /**
	 * Return the number of seconds in simulation time. Only called by old
     * GUI timer so deprecated.
	 * @return An int of number of seconds.
	 */
	public int getSeconds() {
		updateCal();

		return _cal.get( Calendar.SECOND);
	}

	/**
	 * Return the number of minutes in simulation time. Only called by old
     * GUI timer so deprecated.
	 * @return An int of number of minutes
	 */
	public int getMinutes() {
		updateCal();
		return _cal.get( Calendar.MINUTE);
	}

	/**
	 * Return the number of hours (24 hrs) in simulation time. Only called by old
     * GUI timer so deprecated.
	 * @return An int of the number of hours.
	 */
	public int getHours() {
		updateCal();
		return _cal.get( Calendar.HOUR_OF_DAY);
	}


    /** 
     */
    public long getRunningTimeMillis() {
        return (long)(System.currentTimeMillis() - _realTimeSimulationStart - _timeStoppedFor);
    }



}