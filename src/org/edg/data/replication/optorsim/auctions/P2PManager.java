package org.edg.data.replication.optorsim.auctions;

import org.edg.data.replication.optorsim.infrastructure.GridSite;
import org.edg.data.replication.optorsim.time.GridTime;
import org.edg.data.replication.optorsim.time.GridTimeFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * This class manages the P2P network. Each P2PMediator registers
 * itself with the manager when it starts up and objects can get
 * the P2PMediator associated with each GridSite using find().
 * Methods are also provided to shut down the network at the end
 * of the simulation.
 * <p/>
 * Copyright (c) 2004 CERN, ITC-irst, PPARC, on behalf of the EU DataGrid.
 * For license conditions see LICENSE file or
 * <a href="http://www.edg.org/license.html">http://www.edg.org/license.html</a>
 * <p/>
 * 
 * @since JDK1.4
 */
abstract public class P2PManager {

    // A hash of all mappings GridSite -> P2P threads. TODO do we really want this stored like this?
    private static Map _P2PInstances = new HashMap();

    /**
	 * Returns the P2P associated with the specified site, or null. NB we don't
	 * synchonize here as it creates a global block on all P2P threads
	 */
	public static P2P find(GridSite site) {
		return (P2P) _P2PInstances.get( site);
	}

    /**
     * Associate a P2P thread with a site so a call to find() will work
     * @param site  The site on which this P2P thread runs
     * @param p2p The thread to register
     */
	static void register( GridSite site, P2P p2p) {
		synchronized( _P2PInstances) {
			_P2PInstances.put(site, p2p);
		}
	}

    /**
	 * Tell P2P to shut down all Storage Brokers on all P2P sites. This
	 * may block until everything's done.
	 */
	public static void shutdownAllStorageBrokers() {

		for( Iterator iP2P = _P2PInstances.values().iterator(); iP2P.hasNext();) {
			P2P mediator = (P2P) iP2P.next();
			mediator.shutDownAllSBs();
		}
	}

    /**
	 * Shutdown all P2P threads
	 */
	public static void shutdownP2PThreads() {

        GridTime t = GridTimeFactory.getGridTime();
		for( Iterator iP2P = _P2PInstances.values().iterator(); iP2P.hasNext();) {
			P2P mediator = (P2P) iP2P.next();
            mediator.shutDownP2P();
            t.gtJoin(mediator);
		}
	}
}
