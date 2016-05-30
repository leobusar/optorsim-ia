package org.edg.data.replication.optorsim.reptorsim;

/**
 * A NetworkCost represents the estimated cost of a file transfer
 * between two GridSites. It contains a cost, which is the estimated
 * time to transfer the file in seconds, and an associated error in the cost.
 * <p>
 * Copyright (c) 2002 CERN, ITC-irst, PPARC, on behalf of the EU DataGrid.
 * For license conditions see LICENSE file or
 * <a href="http://www.edg.org/license.html">http://www.edg.org/license.html</a>
 * <p>
 * @since JDK1.4
 */
public class NetworkCost {
    private float _cost;
    private float _error;

    public NetworkCost(float cost, float error) {
	_cost = cost;
	_error = error;
    }

    /**
     * Returns the cost in seconds.
     */
    public float getCost() {
	return _cost;
    }

    /**
     * Returns the error in the cost.
     */
    public float getError() {
	return _error;
    }
}
