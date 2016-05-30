package org.edg.data.replication.optorsim.auctions;

import org.edg.data.replication.optorsim.infrastructure.GridSite;

/**
 * This Message is used to send offers to a requesting Auctioneer.
 * <p>
 * Copyright (c) 2002 CERN, ITC-irst, PPARC, on behalf of the EU DataGrid.
 * For license conditions see LICENSE file or
 * <a href="http://www.edg.org/license.html">http://www.edg.org/license.html</a>
 * <p>
 */
public class BidReplyMessage extends Message {

    // the bid offered
    private Bid _bid;

    /**
     * Creates a new BidReplyMessage from the specified parameters.
     */
    public BidReplyMessage(Auction auction, Bid bid) {
		super(auction);	
		_bid = bid;
    }

    /**
     * Return the Bid which is the subject of this Message.
     */
    public Bid getBid() {
		return _bid;
    }
    

	public void handler( P2P p2pMediator) {
		Auction auction = getAuction();
		GridSite auctioningSite = auction.getAuctioneer().getSite();
		GridSite p2pSite = p2pMediator.getSite();
 		
		if (  p2pSite == auctioningSite)	
			auction.addBid( _bid);  // add bid to the auction's list of bids
		else
			p2pMediator.sendMessage( this, auctioningSite);  // forward message to the correct site
	}


}





