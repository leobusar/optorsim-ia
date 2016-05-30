package org.edg.data.replication.optorsim.time;

import java.util.*;

/**
 * TimeAdvancer controls the advance of time in the event driven time
 * model. It runs as a lowest priority thread which in general should
 * only do anything when all other threads are sleeping. When it runs,
 * it continually checks to see if all the other threads are sleeping
 * and if they are, it finds out the soonest time a thread is due to
 * wake up. It advances time to this point and wakes up the thread. The
 * Set of waiting threads is a SortedSet of WaitObjects, sorted by their
 * long value which represents the time they are due to wake up. Thus the
 * first element in the set is always the first thread to wake up.
 * <p>
 * Copyright (c) 2004 CERN, ITC-irst, PPARC, on behalf of the EU DataGrid.
 * For license conditions see LICENSE file or
 * <a href="http://www.edg.org/license.html">http://www.edg.org/license.html</a>
 */
final class TimeAdvancer extends Thread {

	private static ThreadGroup _grp;
    private SortedSet _waitingThreads = new TreeSet();
    private boolean _timePaused = false;

    TimeAdvancer() {
		setPriority(Thread.MIN_PRIORITY);
		setDaemon(true);
	}

    /**
     * Run method of the TimeAdvancer thread.
     */
	public void run() {

        int active = 0, waiting = 0;

        AdvanceableTime _time = (AdvanceableTime) GridTimeFactory.getGridTime();

        // while the ThreadGroup has yet to be assigned
        while( _grp == null || _grp.activeCount() == 0) {
            Thread.yield();
        }
        active = _grp.activeCount();

		while( active > 0){

            if(_timePaused) {
                synchronized(this) {
                    try {
                        wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
			// check the numbers of sleeping & waiting threads in the
			// thread group. It contains all the CE, SB, P2P and
            // AuctionThread threads and the ResourceBroker and Users.
            active = _grp.activeCount();

			synchronized( _waitingThreads) {
				waiting = countWaitingThreads();
			
                if (active == waiting && waiting != 0) {

                    // the first element is the next to be woken up
                    WaitObject wo = (WaitObject)_waitingThreads.first();
                    Object firstWake = wo.getCallingObj();

                    // the first thread to wake should only have Long.MAX_VALUE
                    // if something bad has happened at the end of the simulation
                    // run ie a deadlock.
                    if (wo.longValue() == Long.MAX_VALUE)  continue;

                    // advance time
                    long timeToJump =  wo.longValue() - _time.getTimeMillis();
                    _time.advanceTime(timeToJump);

                    // wake up first thread
                    _time.gtNotify(firstWake);
                }
            }
            Thread.yield();
		}
	}

	private synchronized int countWaitingThreads(){
        int count = 0; 
        for(Iterator it = _waitingThreads.iterator(); it.hasNext();){
            WaitObject waitObj = (WaitObject) it.next() ;
            count+=waitObj.getWaiter();	
        }
        return count ;
    }
	
    /**
     * Set the ThreadGroup of active optorsim threads.
     */
    public static void setThreadGroup(ThreadGroup optorSimThreads) {
        _grp = optorSimThreads;
    }

    /**
     * Adds the thread waiting on waitObj to the set of waiting threads.
     * @param waitObj The Object the thread is waiting on.
     */
    void addWaitingThread(WaitObject waitObj) {
        synchronized (_waitingThreads) {
        	waitObj.addWaiter();
        	if(waitObj.getWaiter() <= 1){
                _waitingThreads.add(waitObj);
            }
        }
    }

    /**
     * Removes the thread waiting on waitObj from the set of waiting threads.
     * @param waitObj The Object the thread is waiting on.
     */
    void removeWaitingThread(WaitObject waitObj) {
        synchronized (_waitingThreads) {
        	if(_waitingThreads.contains(waitObj))
            {
            	waitObj.removeWaiter() ;
                if (waitObj.getWaiter() <= 0 ){
                    _waitingThreads.remove(waitObj);
                }
            }
        }
    }

    /**
     * Wakes up this thread so it can start advancing time again.
     */
    public void startTime() {
        _timePaused = false;
        synchronized(this) {
        	notify();
        }
    }

    /**
     * This method simply makes this thread wait until startTime()
     * is called so effectively time stands still.
     */
    public void stopTime() {
        _timePaused = true;
    }

}
