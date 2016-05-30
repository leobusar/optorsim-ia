package org.edg.data.replication.optorsim.auctions;


/**
 * Class Message has to be extended by all message classes allowing the
 * communication between two P2P Mediators.
 * <p>
 * Copyright (c) 2002 CERN, ITC-irst, PPARC, on behalf of the EU DataGrid.
 * For license conditions see LICENSE file or
 * <a href="http://www.edg.org/license.html">http://www.edg.org/license.html</a>
 * <p>
 */
abstract public class Message {

    // the auction the message is about
    private Auction _auction;

    /**
     * Creates a new Message.
     */
    protected  Message(Auction auction) {
		_auction = auction;
    }
   
    /**
     * The {@link Auction} which is the subject of this Message.
     */
    public Auction getAuction() {
		return _auction;
    }

	/**
	 * Required method describing what the P2P should do on receiving this message
	 * @param p2pMediator the P2P Mediator that received this message
	 */
	abstract void handler( P2P p2pMediator);	
}
