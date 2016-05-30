package org.edg.data.replication.optorsim.reptorsim;

import org.edg.data.replication.optorsim.infrastructure.GridSite;
import org.edg.data.replication.optorsim.infrastructure.GridContainer;

/**
 * This class provides the method to obtain the network costs of
 * transferring files between sites. This is used by some Resource
 * Brokers to find the best site to schedule jobs and some optimisers
 * to locate the best replica of a file.
 * <p>
 * Copyright (c) 2002 CERN, ITC-irst, PPARC, on behalf of the EU DataGrid.
 * For license conditions see LICENSE file or
 * <a href="http://www.edg.org/license.html">http://www.edg.org/license.html</a>
 * <p>
 * @since JDK1.4
 */

public class NetworkClient {

    /**
     * Returns the {@link NetworkCost} of transferring a file of size
     * fileSize between fromSite and toSite.
     */
	public NetworkCost getNetworkCosts( GridSite fromSite, GridSite toSite, int fileSize) {
		float cost;

		// Calculate the available bandwidth
		if(fromSite != toSite) {
			GridContainer gc = GridContainer.getInstance();
		
			cost = BITS_IN_BYTE * fileSize / gc.availableBandwidth( fromSite, toSite);
		}
		else {
			cost = 0;
		}
		
		NetworkCost networkCost=new NetworkCost(cost, 0);
		return networkCost;
	}

	/** Number of bits in a byte (8)*/
    public final static int BITS_IN_BYTE = 8;

}
