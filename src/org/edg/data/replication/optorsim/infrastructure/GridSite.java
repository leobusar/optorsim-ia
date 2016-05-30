package org.edg.data.replication.optorsim.infrastructure;

import org.edg.data.replication.optorsim.time.GridTime;
import org.edg.data.replication.optorsim.time.GridTimeFactory;
import org.edg.data.replication.optorsim.OptorSimOut;

import java.util.*;

import java.lang.Thread;

/**
 * A GridSite keeps track of the CEs and SEs on a site.
 * <p>
 * Copyright (c) 2002 CERN, ITC-irst, PPARC, on behalf of the EU DataGrid.
 * For license conditions see LICENSE file or
 * <a href="http://www.edg.org/license.html">http://www.edg.org/license.html</a>
 * <p>
 * @since JDK1.4
 */
public class GridSite {
	private int _siteID;
	
	private Vector _computingElementCollection = new Vector();
	private Vector _computingThreads = new Vector();
	private Vector _storageElementCollection = new Vector();
	private Set _neighbourSites = null;
	private List _acceptableJobs = new Vector();
	private OptorSimParameters _params = OptorSimParameters.getInstance();
	
	private static int _lastSiteID=0;
	private int _routedFiles = 0;
	private int _accessedFiles = 0;

	/**
	 * Construct a new GridSite object. Its site ID is set to be
	 * 1 higher than the last GridSite.
	 */
	public GridSite() {
		_siteID = _lastSiteID++;

	}
	
	/**
	 * This function asks the GridContainer to find all neighbouring sites
	 * and stores them locally for speed.
	 */
	public void associateNeighbours() {
		GridContainer gc = GridContainer.getInstance();
		_neighbourSites = gc.siteNeighbours( this);
	}
			
	/**
	 * Get the index of this GridSite.
	 * @return the site's index.
	 */
	public int exposeIndex() {
		return _siteID;
	}

	/**
	 * Add a GridJob to the list of acceptable jobs.
	 * @param job the GridJob to add.
	 */
	public void acceptJob( GridJob job) {
		_acceptableJobs.add( job);
	}
	
	/**
	 * Returns an iterator over all the ComputingElements at this site.
	 * @return ListIterator over the CEs.
	 */
	protected ListIterator allCEs() {
		return _computingElementCollection.listIterator();
	}

	/**
	 * Returns an iterator over all the available ComputingElements at this site.
	 * @return ListIterator over the available CEs.
	 */
	protected ListIterator availableCEs() {
		List availableCEs = new LinkedList();  

		for( int i = 0; i < _computingElementCollection.size(); i++) {
			ComputingElement ce = (ComputingElement)_computingElementCollection.get(i);
			if( !ce.getJobHandler().isFull() )
				availableCEs.add(ce);
		}
		return availableCEs.listIterator();
	}

	/**
	 * Check whether a GridJob is in the list of acceptable jobs 
	 * and if so, accept it.
	 * @param job the GridJob to check
	 * @return true if the GridJob is in the list of acceptable jobs,
	 * false otherwise.
	 */
	public boolean acceptsJob( GridJob job) {
		return _acceptableJobs.contains( job);
	}

	/**
	 * Get the list of acceptable jobs for this GridSite.
	 * @return the list of acceptable jobs.
	 */
	public List getJobList() {
		return _acceptableJobs;
	}
	
	/**
	 * Method to print out the current state of the GridSite.
	 */
	public void printState() {
		OptorSimOut.println( "   Site: " + _siteID);
		OptorSimOut.println( "      contains "+_computingElementCollection.size()+ " CEs");
		for( Enumeration eCE = _computingElementCollection.elements(); eCE.hasMoreElements();) {
			ComputingElement ce = (ComputingElement) eCE.nextElement();
			OptorSimOut.println( "            "+ce);
		}

		OptorSimOut.println( "\n      contains "+_storageElementCollection.size()+ " SEs");
		for( Enumeration eSE = _storageElementCollection.elements(); eSE.hasMoreElements();) {
			StorageElement se = (StorageElement) eSE.nextElement();
			OptorSimOut.println( "            "+se+" capacity "+se.getCapacity());
			OptorSimOut.println("            containing "+se.listFiles());
		}
		
	}
	
	/**
	 * Check whether or not there are any StorageElements at this site.
	 * @return true if there is an SE, false otherwise.
	 */
	public boolean hasSEs() {
		return (_storageElementCollection.size() > 0);
	}
	
	/**
	 * Start all the ComputingElement threads at this site running.
	 */
	public void startAllCEs() {
		for(int j=0; j<_computingElementCollection.size(); j++) {
			ComputingElement ce = (ComputingElement)_computingElementCollection.get(j);
			Thread ceThread = new Thread( GridTimeFactory.getThreadGroup(), ce);
			ceThread.start();
			_computingThreads.add( ceThread);
		}
	}	
	
	/**
	 * Called by the {@link GridContainer} when shutting down
	 * ComputingElements, so that they will only shut down
	 * when all their jobs have been processed.
	 */
	public void waitForAllCEsToFinish() {
        GridTime time = GridTimeFactory.getGridTime();
		for(int j=0; j<_computingThreads.size(); j++) {
			Thread ceThread = (Thread) _computingThreads.get (j);
            time.gtJoin(ceThread);
        }
	}
	
	/**
	 * Shut down all the ComputingElements at this GridSite.
	 *
	 */
	public void shutDownAllCEs() {
		for(int j=0; j<_computingElementCollection.size(); j++) {
			ComputingElement ce = (ComputingElement)_computingElementCollection.get(j);
			ce.shutDownCE();
			
			try {
				ce.getJobHandler().interruptJobHandler();
			}
			// in case the CE's thread owns the job handler's monitor
			catch(IllegalMonitorStateException e) {
				System.out.println("GridSite shutDownAllCEs() Error > "+e);
			}

		}		
	}
	
		
	
	/**
	 * Pause all CEs on this site.
	 */
	public void pauseAllCEs() {
		for( int i = 0; i < _computingElementCollection.size(); i++) {
			ComputingElement ce = (ComputingElement)_computingElementCollection.get(i);
			ce.pauseCE();
		}
	}
	
	/**
	 * Unpause all CEs on this site.
	 */
	public void unpauseAllCEs() {
		for( int i = 0; i < _computingElementCollection.size(); i++) {
			ComputingElement ce = (ComputingElement)_computingElementCollection.get(i);
			ce.unpauseCE();
		}
	}
	
	/**
	 * Returns an Enumeration over all the StorageElements at this site.
	 * @return Enumeration over the StorageElements.
	 */
	public Enumeration getSEEnum() {
		return _storageElementCollection.elements(); 
	}

	/**
	 * Method for determining if a particular index corresponds to this GridSite
	 * @param index the number associated with a GridSite
	 * @return true if this site is the referred site, false otherwise.
	 */
	public boolean iAm( int index) {
		return (index == _siteID);
	}
	
	/**
	 * Routine to add a file to one of the registered SEs at this site. The order in which 
	 * the SEs are selected to attempt to store the file is randomised.
	 * @param file The datafile we wish to add
	 * @return PFN of file if file was successfully added, null otherwise
	 */
	public DataFile addFileToRandomSE( DataFile file) {
		boolean successful=false;

        int perms[] = MathSupport.makePermutation( countSEs());
		DataFile storedFile = file.cloneFile();

		for( int i=0; (i < countSEs()) && !successful; i++) {
			StorageElement se = (StorageElement) _storageElementCollection.get( perms[i]);
			successful = se.addFile( storedFile);
		}
        storedFile.releasePin();
		return successful ? storedFile : null;
	}
	
	/**
	 * Method to check if a particular site has a CE with given index as its reference
	 * @param index The CE ID we are looking for
	 * @return true if a CE on this site has the ID
	 */
	public boolean hasCE( int index) {
		for( int i = 0; i <_computingElementCollection.size(); i++) {
			ComputingElement ce = (ComputingElement) _computingElementCollection.get(i);
			if( ce.iAm( index))
				return true;
		}
		return false; 
	}
	
	/**
	 * Method to check if a site has at least one CE
	 * @return true if the site has a CE
	 */
	public boolean hasCE() {
		return _computingElementCollection.size() > 0;
	}

	/**
	 * Counts all the ComputingElements at this site.
	 * @return The number of CEs at this site
	 */
	public int countCEs() {
		return _computingElementCollection.size(); 
	}


	/**
	 * Return the first CE at a site
	 * @return The first CE, or null if site has no CEs
	 */
	public ComputingElement getCE() {
		if( _computingElementCollection.size() > 0)
			return (ComputingElement) _computingElementCollection.get(0);
		else
			return null;
	}

	/**
	  * Return the first SE at a site
	  * @return The first SE, or null if site has no CEs
	  */
	 public StorageElement getSE() {
		 if( _storageElementCollection.size() > 0)
			 return (StorageElement) _storageElementCollection.get(0);
		 else
			 return null;
	 }

	/**
	 * Return the CloseSE for this site. Currently it just returns the first SE
	 * TODO: fix assumption that CloseSE is always the first one.
	 * @return The close SE or null if there isn't one
	 */
	public StorageElement getCloseSE() {
		if(_storageElementCollection.size() > 0) 
			return (StorageElement) _storageElementCollection.get(0);
		else 
			return null;
	}


	/**
	 * Routine to attempt to fill all SEs connected to a particular site with files
	 * @param files the files we wish to fill the SEs with
	 * @return an iterator of PFNs for files successfully stored in the SEs
	 */	
	public Iterator fillSEsWithFiles( Collection files) {
		LinkedList registeredFiles = new LinkedList();

		// Try each SE in turn
		for( int i = 0; i < countSEs(); i++) {
			StorageElement se = (StorageElement) _storageElementCollection.get( i);
			
			// Try filling this SE with all of the files		
			for (Iterator iFile = files.iterator(); iFile.hasNext();) {
				DataFile file = (DataFile) iFile.next();

				if( se.isFull() )
					break;

				DataFile storedFile = file.cloneFile();
                if( se.addFile( storedFile))
					registeredFiles.add( storedFile);
			}
			
			if( ! se.isFull()) 
				System.out.println( "Not enough files to completely fill " + se +
					" ("+100*(1-se.getAvailableSpace() / (float)se.getCapacity())+
					"% filled)");
		}

		return registeredFiles.iterator();
	}
	
	/**
	 * Counts the number of StorageElements at this site.
	 * @return The number of SEs at this site.
	 */
	public int countSEs() {
		if( !hasSEs())
			return 0;
		else	
			return _storageElementCollection.size(); 	
	}

	/**
	 * Increment the counter which keeps track of the number
	 * of file transfers which have been routed via this site.
	 */
	protected void addRoutedFile(){
		_routedFiles++;
	}
    
	/**
	 * Increment the counter which keeps track of the number
	 * of files accessed at this site.
	 */
    protected void addAccessedFile() {
    	_accessedFiles++;
    }
    
    /**
     * Register a new StorageElement at this site.
     * @param se the StorageElement to register.
     * @return the number of SEs now present at the site.
     */
   	public int registerSE( StorageElement se) {
   		_storageElementCollection.add( se);
   		return _storageElementCollection.size();
   	}
   	
   	/**
     * Register a new ComputingElement at this site.
     * @param ce the ComputingElement to register.
     * @return the number of CEs now present at the site.
     */
   	public int registerCE( ComputingElement ce) {
   		_computingElementCollection.add( ce);
   		return _computingElementCollection.size();
   	}

	/**
	 * Get the name of this site.
	 * @return The site name.
	 */
	public String toString() {
		return new String("Site"+_siteID);
	}

	/**
	 * Returns an iterator over all the GridSites neighbouring this
	 * GridSite.
	 * @return An iterator of all sites neighbouring this one.
	 */
	public Iterator neighbouringSites() {
		return _neighbourSites.iterator();
	}
	
	
	/**
	 * Get the {@link Statistics} object for this GridSite.
	 * @return The statistics for this site.
	 */
	public Statistics getStatistics() {
		Map siteStats = new HashMap();
		Set subStats = new HashSet();

			// Get statistics for all site SEs		
		for(Enumeration eSE = getSEEnum(); eSE.hasMoreElements(); ) {
			StorageElement se = (StorageElement) eSE.nextElement();
			subStats.add( se.getStatistics());
		}	
				
		
			//get stats for all CEs at this site
		float totalJobTime = 0;
        float usage = 0;
        long remoteReads = 0, localReads = 0;

		for(Enumeration eCE = _computingElementCollection.elements(); eCE.hasMoreElements(); ) {
			ComputingElement ce = (ComputingElement) eCE.nextElement();
			Statistics ceStats = ce.getStatistics();

			subStats.add( ceStats);

			float ceJobTime = ceStats.getFloatStatistic( "totalJobTime");
            float ceUsage = ceStats.getFloatStatistic("usage");
            long ceLocalReads = ((Long)ceStats.getStatistic("localReads")).longValue();
            long ceRemoteReads = ((Long)ceStats.getStatistic("remoteReads")).longValue();

//			if( totalJobTime < jobTime)
//				totalJobTime = jobTime; 				
			totalJobTime += ceJobTime;
            remoteReads +=ceRemoteReads;
            localReads += ceLocalReads;
            usage += ceUsage;
		}

		siteStats.put("totalJobTime", new Float(totalJobTime));
		if( _params.outputStatistics() == 3){
			siteStats.put("routedFiles", new Integer(_routedFiles));
			siteStats.put("fileAccesses", new Integer(_accessedFiles));
		}
		siteStats.put("ceUsage", new Float(usage));
        siteStats.put("remoteReads", new Long(remoteReads));
        siteStats.put("localReads", new Long(localReads));

		return new Statistics(this, siteStats, subStats);	
	}
}



