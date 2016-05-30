package org.edg.data.replication.optorsim.infrastructure;

/**
 * The Bandwidth factory is used to get Bandwidth instances,
 * either SimpleBandwidth or BackgroundBandwidth depending on 
 * the simulation parameters.
 * <p>
 * Copyright (c) 2004 CERN, ITC-irst, PPARC, on behalf of the EU DataGrid.
 * For license conditions see LICENSE file or
 * <a href="http://www.edg.org/license.html">http://www.edg.org/license.html</a>
 * <p>
 * @since JDK1.4
 */
abstract class BandwidthFactory {

    /**
     * Returns a SimpleBandwidth or BackgroundBandwidth depending on 
     * the simulation parameters
     */
	static Bandwidth getBandwidth( GridSite a, GridSite b, float maxBandwidth) {
		OptorSimParameters params = OptorSimParameters.getInstance();

		if( params.useBackgroundBandwidth()) {
			return new BackgroundBandwidth( a, b, maxBandwidth );
		}
		else
			return new SimpleBandwidth( maxBandwidth);
	}
	
}
