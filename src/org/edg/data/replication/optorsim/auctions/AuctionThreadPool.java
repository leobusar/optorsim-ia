package org.edg.data.replication.optorsim.auctions;

import org.edg.data.replication.optorsim.time.GridTime;
import org.edg.data.replication.optorsim.time.GridTimeFactory;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * The pool of auction threads. 
 * <p>
 * Copyright (c) 2004 CERN, ITC-irst, PPARC, on behalf of the EU DataGrid.
 * For license conditions see LICENSE file or
 * <a href="http://www.edg.org/license.html">http://www.edg.org/license.html</a>
 * <p>
 * @since JDK1.4
 */

abstract public class AuctionThreadPool {
	
	// Maximum number of threads to hold in reserve.
	private static final int _MAX_SPARE_THREADS = 200;
	
	// the pool of auction threads
	private static Set _spareAuctionThreads = new HashSet();
	private static Set _activeAuctionThreads = new HashSet();
    private static GridTime _time = GridTimeFactory.getGridTime();

	/**
	 * Get an auction thread from the pool and start the auction.
	 * @param auction The auction which is to be run.
	 * @return The thread to which the auction is assigned.
	 */
	public static AuctionThread getThread( Auction auction) {
		AuctionThread at;
		synchronized( _spareAuctionThreads) {
 			if( !_spareAuctionThreads.isEmpty()) {
				at =  (AuctionThread) _spareAuctionThreads.iterator().next();
				_spareAuctionThreads.remove( at);
				at.assignAuction( auction);
			} else {
				at = new AuctionThread();
				at.assignAuction( auction);
				at.start();
 			}
	 	}
	 	
	 	synchronized( _activeAuctionThreads) {
	 		_activeAuctionThreads.add( at);
	 	}
							
		return at;
	}
	
	/**
	 * Kill all the auctions.
	 */
	public static void killAllAuctions() {
		synchronized( _activeAuctionThreads) {
			killAuctionThreads( _activeAuctionThreads);
			_activeAuctionThreads.clear();
		}
		synchronized( _spareAuctionThreads) {
			killAuctionThreads( _spareAuctionThreads);
			_spareAuctionThreads.clear();
		}
	}
	
	/**
	 * A wee method for killing off all AuctionThreads in a given set.
	 * @param threadSet The set of AuctionThreads to inhume.
	 */
	private static void killAuctionThreads( Set threadSet) {
		for( Iterator i = threadSet.iterator(); i.hasNext();) {
			AuctionThread at = (AuctionThread) i.next();
			at.die();
            _time.gtJoin(at);
		}
	}
	
	
	/**
	 * Push a thread that isn't used anymore.
	 * @param thread 
	 * @return true if the thread was pushed on the stack, false if the thread should die
	 */
	protected static boolean iShouldStayAlive( AuctionThread thread) {
		
		synchronized( _activeAuctionThreads) {
			_activeAuctionThreads.remove(thread);
		}
		
		synchronized( _spareAuctionThreads) {
			if( _spareAuctionThreads.size() < _MAX_SPARE_THREADS) {
				_spareAuctionThreads.add( thread);
				return true;
			}
		}
				
		return false;
	}
	
}