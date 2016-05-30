package org.edg.data.replication.optorsim.infrastructure;

/**
 * This class holds the mean and standard deviation from the mean 
 * of a bandwidth.
 * <p/>
 * Copyright (c) 2004 CERN, ITC-irst, PPARC, on behalf of the EU DataGrid.
 * For license conditions see LICENSE file or
 * <a href="http://www.edg.org/license.html">http://www.edg.org/license.html</a>
 * <p/>
 * 
 * @since JDK1.4
 */
class BandwidthData {

	/**
	 * Method to get the bandwidth mean stored in this BandwidthData object.
	 * @return the mean bandwidth 
	 */
    float getMean() {
        return _mean;
    }

    /**
     * Method to get the standard deviation stored in this BandwidthData object.
     * @return the standard deviation.
     */
    float getStdev() {
        return _stdev;
    }

    private float _mean;
    private float _stdev;

    /**
     * Instantiate a BandwidthData object with the given mean and
     * standard deviation.
     * @param mean The mean bandwidth to store.
     * @param stdev The standard deviation to store.
     */
    BandwidthData(float mean, float stdev) {

        _mean = mean;
        _stdev = stdev;
    }


}
