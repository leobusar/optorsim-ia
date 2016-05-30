package org.edg.data.replication.optorsim.auctions;

import org.edg.data.replication.optorsim.infrastructure.DataFile;
import org.edg.data.replication.optorsim.infrastructure.GridSite;
import org.edg.data.replication.optorsim.infrastructure.StorageElement;
import org.edg.data.replication.optorsim.time.GridTime;
import org.edg.data.replication.optorsim.time.GridTimeFactory;

import java.util.Map;
import java.util.HashMap;

/**
 * The AccessMediator acts as an Auctioneer, creating an Auction
 * thread for the required file.
 * <p>
 * Copyright (c) 2002 CERN, ITC-irst, PPARC, on behalf of the EU DataGrid.
 * For license conditions see LICENSE file or
 * <a href="http://www.edg.org/license.html">http://www.edg.org/license.html</a>
 * <p>
 */   
public class AccessMediator implements Auctioneer {

    // the site from where the auction is being conducted
    private GridSite _site;
    private String _name;
    private DataFile _winningFile=null;
    private GridTime _time = GridTimeFactory.getGridTime();
    private static Map _amSiteMap = new HashMap();

    /**
     * Constructor called by the optimiser when starting an auction
     */
    private AccessMediator( GridSite site) {
		_site = site;
		_name = "AM@"+site;
    }


	/**
	 * Overload the toString method, giving the AM a more
	 * meaningful name
	 */
	public String toString() {
		return _name;		
	}    

    /**
     * Creates a new Auction and starts it
     * @return The {@link DataFile} for the CE to open
     */
    public DataFile auction(String lfn) {

		Auction auction =  new Auction( lfn, this, 0);
		AuctionThreadPool.getThread( auction);
		
		// wait for auction to give us a result  This blocks the CE
        _time.gtWait(this);
        return _winningFile;
    }

    /**
     * The maximum price the AccessMediator is prepared to pay
     * for a file. Currently there are no budget constraints to 
     * this is simply Float.MAX_VALUE.
     */
		// No budget constraints
    public float getMaxPrice() {
		return Float.MAX_VALUE;
    }

    /**
     * Return the GridSite where this AccessMediator is located.
     */
    	// interface methods
    public GridSite getSite() {
		return _site;
    }

    /**
     * Called by {@link Auction} to find the site to replicate to when replicating as
     * the result of an auction. Replication should never be performed
     * on behalf of an AccessMediator (replication will already have
     * been performed by the {@link StorageBroker} on the site, in the 
     * nested auction) so this returns null. 
     * @return null
     */
	// Never replicate on behalf of a Access Mediator
    public StorageElement getLocalSE() {
		return null;
    }

	/**
	 * When the file is available, remember where it is and wake up the ComputingElement
	 */    
	public void fileAvailable( Auction a, DataFile winningFile) {
		_winningFile = winningFile;
		_time.gtNotify(this);
	}

    /**
     * Create a new AccessMediator for the given GridSite.
     * @param gsite The GridSite to create a new AM for.
     */
    public static void addAM(GridSite gsite) {
        _amSiteMap.put(gsite, new AccessMediator(gsite));
    }

    /**
     * Get the AM at the given site.
     */
    public static AccessMediator getAM(GridSite site) {
        return (AccessMediator) _amSiteMap.get(site);
    }
}
