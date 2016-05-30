package org.edg.data.replication.optorsim.auctions;

import org.edg.data.replication.optorsim.reptorsim.NetworkCost;

/**
 * Represents an offer from a particular SB on a particular site.
 * <p>
 * Copyright (c) 2002 CERN, ITC-irst, PPARC, on behalf of the EU DataGrid.
 * For license conditions see LICENSE file or
 * <a href="http://www.edg.org/license.html">http://www.edg.org/license.html</a>
 * <p>
 */
public class Bid {
    
    //The offering SB
    private StorageBroker _storageBroker;
    
    //offered cost
    private NetworkCost _networkCost;

    
    /**
     * Creates a new Bid.
     */
    public Bid (StorageBroker sb, NetworkCost networkCost) {
		_storageBroker = sb;
		_networkCost = networkCost;
    }
 
    /**
     * The {@link StorageBroker} which is offering this Bid.
     * */
    
    public StorageBroker storageBroker() {
		return _storageBroker;
    }
    
    /**
     * The value of the Bid offered by the bidding StorageBroker. This is 
     * simply the current network cost for transferring that file.
     */
    public float getOffer() {
		return _networkCost.getCost();
    }
    
}
