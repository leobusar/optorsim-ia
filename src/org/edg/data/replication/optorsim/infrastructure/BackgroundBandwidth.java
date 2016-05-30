package org.edg.data.replication.optorsim.infrastructure;

import org.edg.data.replication.optorsim.time.GridTime;
import org.edg.data.replication.optorsim.time.GridTimeFactory;

/**
 * BackgroundBandwidth extends SimpleBandwidth by adding background
 * (non-Grid) traffic to the network as described in the userguide.
 * <p>
 * Copyright (c) 2004 CERN, ITC-irst, PPARC, on behalf of the EU DataGrid.
 * For license conditions see LICENSE file or
 * <a href="http://www.edg.org/license.html">http://www.edg.org/license.html</a>
 * <p>
 * @since JDK1.4
 */

class BackgroundBandwidth extends SimpleBandwidth {
	private GridSite _siteA;
	private GridSite _siteB;
	private GridTime _time;
		
	/**
	 * Initialises the link between siteA and siteB with bandwidth <i> bandwidth</i>.
	 * @param siteA - the source site
	 * @param siteB - the destination site
	 * @param bandwidth - the bandwidth available on the link.
	 */
	BackgroundBandwidth( GridSite siteA, GridSite siteB, float bandwidth) {
		super(bandwidth);

		_siteA = siteA;
		_siteB = siteB;
		
		_time = GridTimeFactory.getGridTime();
	}
				
    /**
     * Returns the available bandwidth for a new connection
     * taking into account the bandwidth used by other non-Grid traffic.
     */
	public float availableBandwidth() {
		
		return getBackgroundBW() / (connectionCount()+1);
	}

    /**
     * Returns the current bandwidth for any ongoing connections
     * taking into account the bandwidth used by other non-Grid traffic.
     */
	public float currentBandwidth() {
		
		return connectionCount() == 0 ? getBackgroundBW() : getBackgroundBW() / connectionCount();
	}

	
    /**
     * Private method to adjust the bandwidth for non-Grid traffic.
     */
    private float getBackgroundBW() {
		
		BandwidthReader bwr = BandwidthReader.getInstance();
		float time = _time.roundedTimeOfDay();
		float newBW;

		//Reduce the static bandwidth according to the underlying traffic.
		//A random jitter is included by taking a random number from the 
		//Landau distribution about the mean bandwidth for that time interval.
		BandwidthData bwResults = bwr.getBandwidth( _siteA, _siteB, time);
		
		float bandwidth = maxBandwidth();
		float mean = bwResults.getMean() * bandwidth;      //mean and stdev are scaled up
		float stdev = bwResults.getStdev() * bandwidth;     //to give absolute values

		do {
		    newBW = (float)MathSupport.addLandauJitter( mean, stdev);
		} while(newBW <= 0 || newBW > bandwidth);
		
		return newBW;
    }
} 
