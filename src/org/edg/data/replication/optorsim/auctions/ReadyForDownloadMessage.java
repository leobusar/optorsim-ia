package org.edg.data.replication.optorsim.auctions;

import org.edg.data.replication.optorsim.infrastructure.GridSite;
import org.edg.data.replication.optorsim.infrastructure.DataFile;

/**
 * This Message is used when a file is ready and is sent
 * to the requesting Auctioneer.
 * <p>
 * Copyright (c) 2002 CERN, ITC-irst, PPARC, on behalf of the EU DataGrid.
 * For license conditions see LICENSE file or
 * <a href="http://www.edg.org/license.html">http://www.edg.org/license.html</a>
 * <p>
 */
public class ReadyForDownloadMessage extends Message {

	DataFile _file;

    /**
     * Creates a new ReadyForDownloadMessage from the specified parameters.
     */
    public ReadyForDownloadMessage(Auction auction, DataFile file) {
		super(auction);	
		_file = file;
    }
    
    /**
     * The DataFile which is ready for download.
     */
    public DataFile file() {
    	return _file;
    }
    
	public void handler( P2P p2pMediator) {

		Auction auction = getAuction();
		GridSite originatingSite = auction.getAuctioneer().getSite();
		
		// if the p2p is at the same site as the auctioneer notify
		// the auction the file is ready
	 	if (p2pMediator.getSite() == originatingSite)
			 auction.receiveRFDMessage( this);
		else
			p2pMediator.sendMessage( this,  originatingSite);  // else send to the correct p2p
	 }
	 
}
