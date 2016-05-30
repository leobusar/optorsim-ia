package org.edg.data.replication.optorsim.time;

import org.edg.data.replication.optorsim.infrastructure.OptorSimParameters;

/**
 * EventDrivenGridTime uses a {@link TimeAdvancer} to control the value of
 * simulation time. Threads calling gtWait, gtSleep or gtJoin are assigned
 * to {@link WaitObject}s which store the time they should wake up
 * (Long.MAX_VALUE when no wake up time is given) and added to a List of
 * waiting threads in the TimeAdvancer. When all the threads are asleep,
 * the TimeAdvancer looks to find the first thread to
 * be woken up, calculates the difference between this time and the
 * current time, advances the time by calling {@link #advanceTime(long)}
 * and wakes up the thread. Using this method of measuring time, it only
 * increases when there is no thread activity, hence the time for calculations
 * is not taken into account. This timing model moves OptorSim towards an
 * event-driven simulation rather than time-driven.
 * <p>
 * Time is initialised at today's date and the time given in the
 * parameters file.
 * <p>
 * Copyright (c) 2004 CERN, ITC-irst, PPARC, on behalf of the EU DataGrid.
 * For license conditions see LICENSE file or
 * <a href="http://www.edg.org/license.html">http://www.edg.org/license.html</a>
 */
public class EventDrivenGridTime extends SimpleGridTime
                            implements AdvanceableTime{
		
	private static EventDrivenGridTime _instance=null;

    private long _timeInMillis = _simulationStartTime;
    private TimeAdvancer _timeAdvancer;

    /**
     * Get the one instance of the singleton class.
     * @return The single GridTime instance.
     */
    public static GridTime getInstance() {
		if( _instance == null)
			_instance = new EventDrivenGridTime();
		return _instance;		
	}

    /**
     * Creates a new instance of EventDrivenGridTime and starts a new
     * TimeAdvancer.
     */
	protected EventDrivenGridTime() {
        super();
        _timeAdvancer = new TimeAdvancer();
        if(OptorSimParameters.getInstance().useGui())
        	_timeAdvancer.stopTime();
        _timeAdvancer.start();
	}

    /**
     * Advances the time by the given number of milliseconds.
     * @param timeToJump The number of ms by which to advance time.
     */
    public void advanceTime(long timeToJump) {
        if (timeToJump < 0) System.out.println("Time running backwards!! "+timeToJump);
        _timeInMillis += timeToJump;
	}

    /**
     * Get the current simulation time in milliseconds. That is, the
     * current simulation Date in milliseconds.
     */
	public long getTimeMillis() {
		return _timeInMillis;
	}

    /**
     * Threads calling this method with Object o are made to wait until
     * {@link #gtNotify(Object o)} is called with the same Object.
     * A WaitObject with long value Long.MAX_VALUE is assigned to the
     * Object. A typical call to this method is time.gtWait(this).
     * @param waitOn Usually the object from which this method was called.
     */
	public void gtWait(Object waitOn) {

        WaitObject waitObj =
                WaitObject.getWaitObject(waitOn, Long.MAX_VALUE);
        addToWaitingList(waitObj);
	}

    /**
     * Threads calling this method with Object o are made to wait until
     * {@link #gtNotify(Object o)} is called with the same Object or they
     * are notified by the TimeAdvancer. A WaitObject with long value
     * timeToWait is assigned to the Object. A typical
     * call to this method is time.gtWait(this, timeToWait).
     * @param waitOn Usually the object from which this method was called.
     * @param timeToWait The time in ms the thread should wait for.
     */
	public void gtWait(Object waitOn, long timeToWait) {

        WaitObject waitObj =
                WaitObject.getWaitObject(waitOn, getTimeMillis() + timeToWait);
        addToWaitingList(waitObj);
	}

    /**
     * Threads calling this method are made to wait on an arbitrary object
     * with a value representing the time it is due to be woken up.
     * The TimeAdvancer will wake the thread up delay ms after this
     * method is called.
     * @param delay The time in ms the thread should sleep for.
     */
    public void gtSleep( long delay) {

        Object o = new Object();
        WaitObject waitObj =
                WaitObject.getWaitObject(o, getTimeMillis() + delay);
        addToWaitingList(waitObj);

        // to save on memory remove these WaitObjects from the map
        WaitObject.discard(o);
    }

    /**
     * Wakes up the thread which called a gtWait() method with
     * Object waitOn.
     * @param waitOn The Object with which gtWait() was called.
     */
	public void gtNotify(Object waitOn) {

        // obj has never called wait
        if (WaitObject.getWaitObject(waitOn) == null) return;

        synchronized(waitOn) {
			waitOn.notify();
            _timeAdvancer.removeWaitingThread(WaitObject.getWaitObject(waitOn));
		}
	}

    /**
     * Registers the calling Thread as a waiting thread with the
     * TimeAdvancer and waits for Thread t to end.
     * @param t The Thread for which the calling Thread is waiting to end.
     */
    public void gtJoin(Thread t) {

        WaitObject w =
                WaitObject.getWaitObject(new Object(), Long.MAX_VALUE);
        _timeAdvancer.addWaitingThread(w);
        super.gtJoin(t);
        _timeAdvancer.removeWaitingThread(w);
    }

    /**
     * Tells the TimeAdvancer to stop advancing time.
     */
    public void start() {
        _timeAdvancer.startTime();
    }

    /**
     * Tells the TimeAdvancer to start advancing time again.
     */
    public void stop() {
        _timeAdvancer.stopTime();
    }

    /**
     * Registers the calling thread as a waiting thread with the
     * TimeAdvancer and then waits on it.
     */
    private void addToWaitingList(WaitObject waitObj) {

		try {
            synchronized(waitObj.getCallingObj()) {

               _timeAdvancer.addWaitingThread(waitObj);
               waitObj.getCallingObj().wait();
            }
		} catch ( InterruptedException e) {
				System.out.println( "Thread "+Thread.currentThread()+" was interrupted whilst sleeping...");
		}
    }

    /**
     */
    public long getRunningTimeMillis() {
        return _timeInMillis - _simulationStartTime;
    }

}
