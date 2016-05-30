package org.edg.data.replication.optorsim.auctions;

import org.edg.data.replication.optorsim.infrastructure.GridSite;
import org.edg.data.replication.optorsim.infrastructure.DataFile;

/**
 * This interface is implemented by the {@link AccessMediator} and
 * {@link StorageBroker} and provides methods of communication
 * between these and the {@link Auction}.
 * <p>
 * Copyright (c) 2002 CERN, ITC-irst, PPARC, on behalf of the EU DataGrid.
 * For license conditions see LICENSE file or
 * <a href="http://www.edg.org/license.html">http://www.edg.org/license.html</a>
 * <p>
 */
public interface Auctioneer {

    /**
     * @return The Auctioneer site
     */
    public GridSite getSite();

    /**
     * @return The maximum price this Auctioneer will pay for the file
     */
    public float getMaxPrice();


	/**
	 * A call-back method used by the Auction to notify the Auctioneer when the 
	 * file is available.  If noone won the auction, winningFile is null.  The Auction
	 * class must guarantee it calls auctionComplete before calling this method.
	 * @param auction The auction in question
	 * @param winningFile the DataFile of the winning replica, or null if noone won.
	 */    
    public void fileAvailable( Auction auction, DataFile winningFile);
}
