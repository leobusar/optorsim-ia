package org.edg.data.replication.optorsim.infrastructure;

/**
 * A Bandwidth represents a network link between two adjacent
 * GridSites. Methods such as adding and dropping connections and
 * querying the current and maximum bandwidth must be implemented.
 * <p>
 * Copyright (c) 2004 CERN, ITC-irst, PPARC, on behalf of the EU DataGrid.
 * For license conditions see LICENSE file or
 * <a href="http://www.edg.org/license.html">http://www.edg.org/license.html</a>
 * <p>
 * @since JDK1.4
 *
 */
public interface Bandwidth {
	
	/**
	 * Add an extra connection to this network segment
	 */
	void addConnection();
		
	/**
	 * Remove a connection on this network segment
	 */
	void dropConnection();

	/**
	 * Find out what network connection would be available on this
	 * network segment for a potential file transfer.
	 * @return The bandwidth that would be available for a file transfer.
	 */	
	float availableBandwidth();
	
    /**
     * Returns the bandwidth available at the moment for file
     * transfers currently ongoing on this link.
     * @return The bandwidth available for current file transfers.
     */
    float currentBandwidth();

	/**
	 * Return the maximum available bandwidth for this network segment
	 * @return Bandwidth available if only one connection is utilising this link
	 */
	float maxBandwidth();
	
	/**
	 * Enquire the number of connections for this network segment
	 * @return Number of concurrent connections for this network segment
	 */
	int connectionCount();
}
