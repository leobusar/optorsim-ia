package org.edg.data.replication.optorsim.time;

import org.edg.data.replication.optorsim.time.EventDrivenGridTime;
import org.edg.data.replication.optorsim.time.GridTime;
import org.edg.data.replication.optorsim.infrastructure.OptorSimParameters;

/**
 * Factory class for producing the right kind of GridTime object - 
 * SimpleGridTime or EventDrivenGridTime, which hops to the next
 * event if all threads are inactive.
 * 
 * Copyright (c) 2004 CERN, ITC-irst, PPARC, on behalf of the EU DataGrid.
 * For license conditions see LICENSE file or
 * <a href="http://www.edg.org/license.html">http://www.edg.org/license.html</a>
 * <p>
 */
public class GridTimeFactory {

    private static ThreadGroup _threadGroup = new ThreadGroup("optorsim threads");

	/**
	 * Get the version of GridTime currently being used.
	 */
	public static GridTime getGridTime() {
		
		OptorSimParameters params = OptorSimParameters.getInstance();
		
		boolean adv = params.advanceTime();
		if( adv) {
			return EventDrivenGridTime.getInstance();
		//	return SimpleGridTime.getInstance();
		}
		else {
			return SimpleGridTime.getInstance();
		}

	}

	/** Get the ThreadGroup which contains the ComputingElement and ResourceBroker threads.*/
    public static ThreadGroup getThreadGroup() {
        return _threadGroup;

    }

    /**
     * Let the {@link TimeAdvancer} know which ThreadGroup 
     * contains the ComputingElement and ResourceBroker threads.
     * @param tg The ThreadGroup to pass to the TimeAdvancer.
     */
    public static void setThreadGroup(ThreadGroup tg) {
        _threadGroup = tg;
        TimeAdvancer.setThreadGroup(tg);
    }

}
