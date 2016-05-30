package org.edg.data.replication.optorsim.time;

/**
 * Defines a method for advancing simulation time. Using implementations
 * of this interface means the increase of time can be controlled.
 * <p>
 * Copyright (c) 2004 CERN, ITC-irst, PPARC, on behalf of the EU DataGrid.
 * For license conditions see LICENSE file or
 * <a href="http://www.edg.org/license.html">http://www.edg.org/license.html</a>
 * <p>
 * @since JDK1.4
 */
interface AdvanceableTime extends GridTime{

    /**
     * Advances time by time ms.
     * @param time The number of ms by which to advance time.
     */
    void advanceTime(long time);
}
