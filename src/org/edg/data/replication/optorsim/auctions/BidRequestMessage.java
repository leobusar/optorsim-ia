package org.edg.data.replication.optorsim.auctions;

/**
 * This Message is used to propagate the request for a file.
 * <p>
 * Copyright (c) 2002 CERN, ITC-irst, PPARC, on behalf of the EU DataGrid.
 * For license conditions see LICENSE file or
 * <a href="http://www.edg.org/license.html">http://www.edg.org/license.html</a>
 * <p>
 */
public class BidRequestMessage extends PropagatingMessage {
    
    /**
     * Creates a new BidRequestMessage from the specified parameters.
     */
    public BidRequestMessage(Auction auction) {
		super(auction);

	}
    
    /**
     * This is the entry point, called by the P2P mediator
     */
    public void handler( P2P mediator) {

		/**
		 *  Send them message to the local StorageBrokers, but not the originating
		 *  StorageBroker, if one of the local ones originated the auction.
		 */
		mediator.sendToLocalSBs( this, false);

			// Deal with message propagation
		super.handler(mediator);
    }

}
