package org.edg.data.replication.optorsim.infrastructure;

import java.util.*;

// TODO: more imports from unaccessible packages...
import org.edg.data.replication.optorsim.auctions.P2P;
import org.edg.data.replication.optorsim.auctions.P2PManager;

/**
 * This class keeps track of GridSites and the replicate and copy 
 * methods are used to transfer files from one site to another.
 * <p>
 * Copyright (c) 2002 CERN, ITC-irst, PPARC, on behalf of the EU DataGrid.
 * For license conditions see LICENSE file or
 * <a href="http://www.edg.org/license.html">http://www.edg.org/license.html</a>
 * <p>
 * @since JDK1.4
 */
public class GridContainer {

	private Vector _allGridSites = new Vector();
	private Set _allJobs = new HashSet();
	private double _totalProb = 0;
	private static GridContainer _instance = null;
	private Random _randGen;
	
	private Map _bandwidth = Collections.synchronizedMap(new HashMap());
	private int _totalNumberOfCEs = 0;
	private Map _predecessors = Collections.synchronizedMap( new HashMap());
	
	private long _replications = 0;
	private int _jobID = 0;

	/**
	 *   STATIC METHODS
	 */

	/** 
	 * Singleton;  get the single instance of this object 
	 * @return The GridContainer instance.
	 */
	public static GridContainer getInstance(){
		if( _instance == null)
			_instance = new GridContainer();
		return _instance;
	}
	
	
	
	private GridContainer()
	{
		// Private constructer for this class
		if( OptorSimParameters.getInstance().useRandomSeed())
			_randGen = new Random();
		else
			_randGen = new Random(100L);
	}
	
	
	/**
	 * Return the Bandwidth object for the segment between siteA and siteB
	 * @param siteA First site in connection
	 * @param siteB Second site in connection
	 * @return the Bandwidth object connecting both sites, or null if there isn't one
	 */
	public Bandwidth networkSegment( GridSite siteA, GridSite siteB) {
		GridSitePair gsp = new GridSitePair( siteA, siteB);
		return (Bandwidth) _bandwidth.get( gsp);
	}
	
	/**
	 * Return the Bandwidth object for the GridSitePair gsp
	 * @param gsp The GridSitePair object describing the two GridSites
	 * @return the Bandwidth object describing the network link between
	 * the two sites, or null if they have no direct connection
	 */
	public Bandwidth networkSegment( GridSitePair gsp) {
		return (Bandwidth) _bandwidth.get(gsp);
	}
		
	/**
	 * Go through the best route from siteA to siteB, incrementing the number of network
	 * connections at each site
	 */
	public void addConnection( GridSite siteA, GridSite siteB) {
		List route = networkRoute( siteA, siteB);
		List routers = getRouters( siteA, siteB);
		
		for( Iterator i = route.iterator(); i.hasNext(); ) {
			GridSitePair gsp = (GridSitePair) i.next();
			networkSegment( gsp).addConnection();
		}			
		for( int j=0; j<routers.size(); j++) 
			((GridSite)routers.get(j)).addRoutedFile();
		siteA.addAccessedFile();
	}
	
	/**
	 * Go through the best route from siteA to siteB, decreasing the number of network
	 * connections at each site
	 */
	public void dropConnection( GridSite siteA, GridSite siteB) {
		List route = networkRoute( siteA, siteB);
		
		for( Iterator i = route.iterator(); i.hasNext();) {
			GridSitePair gsp = (GridSitePair) i.next();
			networkSegment( gsp).dropConnection();			
		}			
	}

	
	/**
	 *  Go through each site in optimal route from siteA to siteB, keeping track of the
	 *  minimum bandwith available.  This is returned as the available bandwidth
	 * between the two sites for potential file transfers.
	 */
	public float availableBandwidth( GridSite siteA, GridSite siteB) {
		float minBW = Float.POSITIVE_INFINITY;
		boolean first = true;

		List route = networkRoute( siteA, siteB);
		
		for( Iterator i = route.iterator(); i.hasNext();) {
			GridSitePair gsp = (GridSitePair) i.next();
			float bw = networkSegment( gsp).availableBandwidth();			
			
			if( first || (bw < minBW)) {
				minBW = bw;
				first = false;
			}		
		}

		return minBW;		
	}
	
	/**
	 *  Go through each site in optimal route from siteA to siteB, keeping track of the
	 *  minimum bandwith currently in use. This is returned as the current bandwidth
	 * between the two sites for ongoing file transfers.
	 */
	public float currentBandwidth( GridSite siteA, GridSite siteB) {
		float minBW = Float.POSITIVE_INFINITY;
		boolean first = true;

		List route = networkRoute( siteA, siteB);
		
		for( Iterator i = route.iterator(); i.hasNext();) {
			GridSitePair gsp = (GridSitePair) i.next();
			float bw = networkSegment( gsp).currentBandwidth();
			
			if( first || (bw < minBW)) {
				minBW = bw;
				first = false;
			}		
		}

		return minBW;		
	}
	
	/**
	 * Private method for pulling out the route from siteA to siteB
	 * @param siteA the initial site.
	 * @param siteB the destination site.
	 * @return A List of GridSitePairs in order from siteA to siteB
	 */
	private List networkRoute( GridSite siteA, GridSite siteB) {
		List route = new LinkedList();
		
		Map pred = (Map) _predecessors.get(siteB);		
		
		GridSite site2 = siteA;
		do {
			GridSite site1=site2;
			site2 = (GridSite) pred.get(site1);
			GridSitePair gsp = new GridSitePair( site1, site2);
			route.add( gsp);
		} while( site2 != siteB);

		return route;		
	}
	
	/**
	 * Private method for getting the router sites on a network route
	 * @param siteA The source site
	 * @param siteB The destination site
	 * @return Ordered list of GridSites between siteA and siteB
	 */
	private List getRouters( GridSite siteA, GridSite siteB ) {
		List routers = new LinkedList();
		Map pred = (Map)_predecessors.get( siteB);
		
		for( GridSite site2 = (GridSite)pred.get(siteA); site2 != siteB; ) {
			routers.add( site2);	
			GridSite site1 = site2;
			site2 = (GridSite)pred.get( site1);		
		}
		return routers;
	}

	/**
	 * A method for dumping the current state of the Grid to the terminal
	 *
	 */
	public void printState() {
		System.out.println( "GridContainer> Current state of the Grid");
		System.out.println( "    There are "+_allGridSites.size()+" sites configured:\n");		
		for( Enumeration eSite = _allGridSites.elements(); eSite.hasMoreElements(); ) {
			GridSite site = (GridSite) eSite.nextElement();
			site.printState();
		}
		
	}

	
	/**
	 *  Go through each site in optimal route from siteA to siteB, keeping track of the
	 *  minimum pair-wise maximum-bandwith.  This is returned as the maximum available
	 * (or potential) bandwidth between the two sites
	 */
	public float maxBandwidth( GridSite siteA, GridSite siteB) {

		float maxBW = Float.POSITIVE_INFINITY;
		boolean first = true;

		List route = networkRoute( siteA, siteB);
		
		for( Iterator i = route.iterator(); i.hasNext();) {
			GridSitePair gsp = (GridSitePair) i.next();
			float bw = networkSegment( gsp).maxBandwidth();			
			
			if( first || (bw < maxBW)) {
				maxBW = bw;
				first = false;
			}		
		}

		return maxBW;		
	}
	

	/**
	 * Method to register a best-network-route Map for a given source Site
	 * @param site Site that we're concerned with here
	 * @param pred The Predecessor Map
	 * @param neighbouringBW maps between neighbouring sites and the link bandwidth
	 */	
	public void addNetworkRoutes( GridSite site, Map pred, Map neighbouringBW) {
		/**
		 *  The Predecessor map takes a GridSite and returns a GridSite. By taking the returned site
		 * and feeding it into the Map again and doing this until one gets the source-site, the best
		 * route is described.
		 */
		_predecessors.put(site, pred);
		
			// For each neighbouring site (nSite)
		for( Iterator iSite = neighbouringBW.keySet().iterator(); iSite.hasNext();) {
			GridSite nSite = (GridSite) iSite.next();
			GridSitePair gsp = new GridSitePair( site, nSite);
			float bw = ((Float) neighbouringBW.get(nSite)).floatValue();
			
			if( _bandwidth.containsKey( gsp)) {
				Bandwidth storedBW = (Bandwidth) _bandwidth.get(gsp);
				if( storedBW.maxBandwidth() != bw)
					System.out.println( "Inconsistency detected in bandwidth between "+site+" and "+nSite+
												  " ("+bw+" != "+storedBW.maxBandwidth());
			} else {
				Bandwidth link = BandwidthFactory.getBandwidth( site, nSite, bw);
				_bandwidth.put(gsp, link);				
			}
		}
		
	}
	
	
	/**
	 * Returns a List of CEs for which the queue size is less than the maximum allowed.
	 * @return the List of free CEs
	 */
	public ListIterator freeCEs() {
		List listOfCEs = new LinkedList();
		
		for( Enumeration eSite = allGridSites();
				eSite.hasMoreElements();) {
			GridSite site = (GridSite) eSite.nextElement();
			
			for( ListIterator li = site.availableCEs(); li.hasNext();) {
				listOfCEs.add( (ComputingElement) li.next());
			}
		}
        
		return listOfCEs.listIterator();
	}

	/**
	 * Find all the GridSites that are directly connected to the named site
	 * @param site The site under consideration
	 * @return A set of neighbouring sites.
	 * TODO: this should only be used in GridSite, and a call made from there
	 * TODO: for performance, this could be stored as a Map between site and a Set of GridSites
	 */
	public Set siteNeighbours( GridSite site) {
		Set neighbours = new HashSet();

		Map pred = (Map)_predecessors.get( site);

		for( Enumeration eSite = allGridSites(); eSite.hasMoreElements();) {
			GridSite potential = (GridSite) eSite.nextElement();
			if( pred.get( potential) == site)
				neighbours.add( potential);
		}
		
		return neighbours;		
	}

	/**
	 * Returns an enumeration over all the GridSites in the GridContainter.
	 * @return Enumeration over the GridSites.
	 */
	// TODO: Make this protected (or private)	
	public Enumeration allGridSites(){
		return _allGridSites.elements();
	}

	/**
	 * This will tell all CEs to finish.  The method will block until all the threads are finished
	 */	
	public void shutDownAllCEs() {
		for( int i=0; i < _allGridSites.size();i++) {
			GridSite gsite = (GridSite)_allGridSites.get(i);
			gsite.shutDownAllCEs();
		}

		for( int i=0; i < _allGridSites.size();i++) {
			GridSite gsite = (GridSite)_allGridSites.get(i);
			gsite.waitForAllCEsToFinish();
		}		
	}


	/**
	 * Add a site to the overall Grid "Container"
	 * @param gs The GridSite we wish to add
	 */
	public void addSite( GridSite gs) {
		_allGridSites.add( gs);
		if( gs.hasCE())
			_totalNumberOfCEs++;
	}

	/**
	 * Starts all the {@link P2P}s at the GridSites.
	 *
	 */
	public void startAllP2P() {
		for( Enumeration eSite = allGridSites(); eSite.hasMoreElements();) {
			GridSite site = (GridSite) eSite.nextElement();
			new P2P( site);
		}
	}

	/**
	 * Shuts down all the {@link P2P}s at the GridSites. 
	 * The StorageBrokers are shut down first, then the P2P system.
	 */
	public void shutDownAllP2P() {
			// first, force the system to a stable state
		P2PManager.shutdownAllStorageBrokers();

			// Then take down the P2P system.
		P2PManager.shutdownP2PThreads();
	}


	/**
	 * Tell GridContainer to inform all Sites to start their CEs
	 */
	public void startAllCEs() {
		
		for( int i=0; i < _allGridSites.size();i++) {
			GridSite gsite = (GridSite)_allGridSites.get(i);
			gsite.startAllCEs();
		}
	}


	/**
	 * Allow calling routine to iterate over all GridSites
	 * @return An iterator over all GridSites
	 */	
	public Iterator iterateGridSites() {
		return _allGridSites.iterator();
	}


	/**
	 * Find the GridSite with a specified reference ID
	 * @param id The GridSite reference to look for.
	 * @return The GridSite if found, null otherwise
	 */
	public GridSite findGridSiteByID( int id) {
		for( int i=0; i < _allGridSites.size(); i++) {
			GridSite gsite = (GridSite) _allGridSites.get(i);
			if( gsite.iAm( id))
				return gsite;
		}
		return null;
	}
	
	/**
	 * Pause all CEs in existence.
	 */
	public void pauseAllCEs() {
		for( int i=0; i < _allGridSites.size(); i++) {
			GridSite gsite = (GridSite) _allGridSites.get(i);
			gsite.pauseAllCEs();
		}		
	}

	/**
	 * Unpause all CEs in existence.
	 */
	public void unpauseAllCEs() {
		for( int i=0; i < _allGridSites.size(); i++) {
			GridSite gsite = (GridSite) _allGridSites.get(i);
			gsite.unpauseAllCEs();
		}		
	}


    /**
     * Method to transfer the file from one site to another and create a 
     * copy of the file in the destination SE. It gets a 
FileTransfer
     * object from the FileTransferFactory and uses it to move the file.
     * @return The replica if successful, null otherwise
     */
    public DataFile replicate(DataFile fromFile, StorageElement toSE) {    						
		GridSite fromSite = fromFile.se().getGridSite();
		GridSite toSite = toSE.getGridSite();

		fromFile.addPin();
		
		// Make sure space stays available on SE
		if( ! toSE.reserveSpace( fromFile.size())) {
			fromFile.releasePin();
			return null;
		}

		FileTransfer ft = FileTransferFactory.getFileTransfer();
	    ft.transferFile(fromSite, toSite, fromFile.size());
		fromFile.se().accessFile(fromFile);
	    // clone file and add file to SE unless no replication is possible.
	    DataFile replica = fromFile.cloneFile();
	    toSE.addPreReservedFile( replica);

	    // unpin original file
	     fromFile.releasePin();
	    
	    //keep count of total number of replications
	    synchronized (this) {
		_replications++;
	    }

	    return replica;
    }

    
	/**
	 * Request a statistics tree for the current state of the simulation.
	 * @return The head statistic (for GridContainer).
	 */
	public Statistics getStatistics() {
		Map allStats = new HashMap();
		Set subStats = new HashSet();
		float totalJobTime = 0;
        float ceUsage = 0;
        long remoteReads = 0, localReads = 0;

			// Build up the Set of child statistics
		for(Enumeration eSites = allGridSites(); eSites.hasMoreElements();) {
			GridSite site = (GridSite)eSites.nextElement();
			Statistics siteStats = site.getStatistics();
			subStats.add( siteStats);
			float jobTime = ((Float)siteStats.getStatistic("totalJobTime")).floatValue();
            float siteCEUsage = ((Float)siteStats.getStatistic("ceUsage")).floatValue();
            long siteLocalReads = ((Long)siteStats.getStatistic("localReads")).longValue();
            long siteRemoteReads = ((Long)siteStats.getStatistic("remoteReads")).longValue();

//			if( totalJobTime < jobTime)
//				totalJobTime = jobTime; 
			totalJobTime += jobTime;
            ceUsage += siteCEUsage;
            localReads += siteLocalReads;
            remoteReads += siteRemoteReads;
		}
		
			// Add our own statistics.
		allStats.put("totalJobTime", new Float(totalJobTime));
		allStats.put("ceUsage", new Float(ceUsage/(float)_totalNumberOfCEs));
        allStats.put("replications", new Long(_replications));
        allStats.put("localReads", new Long(localReads));
        allStats.put("remoteReads", new Long(remoteReads));
        allStats.put("ENU", new Float((float)(_replications+remoteReads)/(float)(localReads+remoteReads)));

			// Return the head node.
		return new Statistics( this, allStats, subStats);
	}

	/**
	 * Overload the toString method to contain GridContainer specific label
	 */
	public String toString() {
		return "the GridContainer";
	}
	
    /**
     * Used to transfer file if no replication is taking place, using
     * the same file transfer mechanism as replicate() but does not
     * add the file to the destination SE.
     */
    public void copy( DataFile fromFile, GridSite toSite, float fileFraction) {
		
		if( fromFile == null) {
			System.out.println(" ERROR> Replication failed."+
							   "  File as specified by "+fromFile+
							   " does not exist.");
			return;
		}

		StorageElement fromSE = fromFile.se();
		
		fromFile.addPin();
		
		GridSite fromSite = fromSE.getGridSite();
		
		FileTransfer ft = FileTransferFactory.getFileTransfer();
		ft.transferFile(fromSite, toSite, (int) (fromFile.size()*fileFraction));
		fromSE.accessFile(fromFile);
		//unpin original file
		fromFile.releasePin();
		
	}
	
	
	/**
	 * Returns the total number of known CEs.
	 * @return an integer representing total number of CEs
	 */
	public int countCEs(){
		return _totalNumberOfCEs;
	}
	
    /**
     * Returns the total number of site on the Grid.
     */
    public int numberOfSites() {
	return _allGridSites.size();
    }

	/**
	 * Add a new job to the set of all available jobs
	 * @param job - a new job to add 
	 */
	public void registerJob( GridJob job) {	
		_allJobs.add(job);
		_totalProb += job.probability();
	}

	/**
	 * Find a random job to run, based on probability of each job
	 * @return a job if one could be found, null otherwise.
	 */
	public GridJob randomJob() {
		GridJob job=null;
		double lowerBound=0;
		double rand = _randGen.nextDouble()* _totalProb;

		for( Iterator iJ = _allJobs.iterator(); iJ.hasNext();) {
				job = ((GridJob)iJ.next()).cloneJob();
				lowerBound += job.probability();
				if( lowerBound > rand)
					break;
		}
		//TODO: Assigning each job a unique ID is a temporary fix until
		//we sort something better out. 
		_jobID++; 	 
		job.assignID(_jobID);
		// remove unwanted files from job, if only a subset is required
		if (job.getFileFraction() < 1){
			job.trimJob();
			job.setFileFraction(1);
		}
		return job;
	}
}
