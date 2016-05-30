package org.edg.data.replication.optorsim.auctions;

import org.edg.data.replication.optorsim.time.GridTime;
import org.edg.data.replication.optorsim.time.GridTimeFactory;

/** 
 * Auction threads form a pool of threads which sleep when they are not being
 * used. This class runs one thread, to which auctions can be assigned as long as the
 * thread is alive. 
 * <p>
 * Copyright (c) 2004 CERN, ITC-irst, PPARC, on behalf of the EU DataGrid.
 * For license conditions see LICENSE file or
 * <a href="http://www.edg.org/license.html">http://www.edg.org/license.html</a>
 * <p>
 * @since JDK1.4
*/

class AuctionThread extends Thread {

	static private int _threadCounter=0;

	private Auction _myAuction = null;
	private boolean _iAmAlive = true;
    private GridTime _time = GridTimeFactory.getGridTime();
    private RFDStatusHandler _rfdStatusHandler;

    public AuctionThread() {
        super(GridTimeFactory.getThreadGroup(),"AuctionThread"+_threadCounter++);
        _rfdStatusHandler = new RFDStatusHandler();
	}
	
	/**
	 * Assign an auction to this thread.
	 * @param auction The auction which this thread is to run.
	 */
	void assignAuction( Auction auction) {
		_myAuction = auction;
        auction.assignRFDSH(_rfdStatusHandler);
		_time.gtNotify(this);
	}

	/**
	 * Kill this auction thread.
	 */
	public void die() {
		_iAmAlive = false;
		_time.gtNotify(this);
	}
	
	/**
	 * Run this auction thread until it is told to die. 
	 * While running, it will process an auction then return to the thread pool 
	 * and wait until it is assigned another auction.
	 */
	public void run() {
		
		Debugger.printDebugMessage( super.getName()+"> kicking into life processing "+_myAuction+" ...");

		while( _iAmAlive) {
			
            // All Auction-specific code is in the Auction method
            _myAuction.doAuction();

            _iAmAlive = AuctionThreadPool.iShouldStayAlive( this);

            if( _iAmAlive) _time.gtWait(this);
        }
	}

}