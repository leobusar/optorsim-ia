package org.edg.data.replication.optorsim.auctions;

import org.edg.data.replication.optorsim.infrastructure.GridSite;
import org.edg.data.replication.optorsim.infrastructure.OptorSimParameters;

import java.util.Set;
import java.util.HashSet;
import java.util.Iterator;

/**
 * This Message is used to propagate an arbitrary message to all sites.  Specific messages
 * should overload the handler method, making sure to call super().
 * <p>
 * Copyright (c) 2002 CERN, ITC-irst, PPARC, on behalf of the EU DataGrid.
 * For license conditions see LICENSE file or
 * <a href="http://www.edg.org/license.html">http://www.edg.org/license.html</a>
 * <p>
 */
abstract public class PropagatingMessage extends Message implements Cloneable {
    
    // This counter regulates the propagation of a call for
    // bid in a single auction
    private int _hopCount;

    // a vector containing the sites already contacted by this message
    private Set _contactedSites = new HashSet();
  
    /**
     * Creates a new BidRequestMessage from the specified parameters.
     */
    public PropagatingMessage(Auction auction) {
		super(auction);
		_hopCount = OptorSimParameters.getInstance().getInitialHopCount();
	}

    /****    Suitable get and set methods   *****/ 
	private synchronized boolean shouldPropagateMsg() {
		return _hopCount > 0;
	}
	
    private synchronized void decreaseHopCount() {
		_hopCount--;
    }

    private void addContactedSite( GridSite site) {
    	synchronized( _contactedSites) {
			_contactedSites.add(site);
    	}
    }

    /**
	 * This method is a hybrid of the addContactedSite() and hasContacted() methods.
	 * If a site hasn't been contacted, then it will be marked as contacted in an atomic
	 * operation.
	 * @param site The GridSite under consideration
	 * @return true if the site has been contacted, false otherwise
	 */
	private boolean testAndSetHasContacted( GridSite site) {
		synchronized( _contactedSites) {
			if( _contactedSites.contains(site))
				return true;
				
			_contactedSites.add(site);
		}
		return false;
	}
    
    /**
     * Send a message to all neighbouring sites that haven't
     * already received this message.  The message send is a
     * clone()ed copy, which has the same _contactedSites.
     * @param mediator The local P2P mediator to use.
     */
	private void propagateMsg( P2P mediator ) {
		decreaseHopCount();
		
		for( Iterator i = mediator.getSite().neighbouringSites(); i.hasNext();) {
			GridSite nSite = (GridSite) i.next();
		    
			if(! testAndSetHasContacted(nSite))
				mediator.sendMessage( (PropagatingMessage) this.clone(), nSite);
		}
	}



	/**
	 * Used to maintain a seperate hopcount for each iteration.  We cheat here
	 * by maintaining a global _contactedSites list, which each P2P mediator updates
	 * via the addContactedSite() method (indirectly)
	 */
	public Object clone() {
		try{
			PropagatingMessage newPM = (PropagatingMessage) super.clone();
			synchronized( _contactedSites) {
				newPM._contactedSites = _contactedSites;
			}
			return newPM;
		}
		catch( CloneNotSupportedException e) {
			// This can't happen. honest!
			throw new InternalError(e.toString());
		}
	}
	
    
    /**
     * This is the entry point, called by the P2P mediator. Classes that
     * extend this method must call this method for message propagation
     * to work.
     */
    public void handler( P2P mediator) {
		Auction auction = getAuction();
		GridSite thisSite = mediator.getSite();
		GridSite origSite = auction.getAuctioneer().getSite();
	
			/**
			 * The propagateMsg method mostly deals with updating the contact list.
			 * The noticable exception is the first P2P site, which is dealt with here.
			 */
		if( origSite == thisSite)
			addContactedSite( thisSite);

			// Propagates the call for bid to close sites by forwarding the
			// message
		if( shouldPropagateMsg())
			propagateMsg( mediator);
    }
    
}
