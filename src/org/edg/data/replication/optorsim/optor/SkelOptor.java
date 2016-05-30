package org.edg.data.replication.optorsim.optor;

import org.edg.data.replication.optorsim.infrastructure.*;
import org.edg.data.replication.optorsim.reptorsim.*;
import java.util.Random;

/**
 * This class is the basic implementation of the {@link Optimisable}
 * interface. It provides the basic operations required for all the 
 * optimisation classes and simple versions of the two most important
 * methods: getAccessCost() and getBestFile(). getAccessCost()
 * calculates the expected cost to access the specified files from the
 * specified ComputingElement using network information to work out the
 * best replica to use for each file in terms of network latencies.
 * <p>
 * getBestFile() uses the same network information to return, for each
 * file given, the replica that can be accessed from the Computing Element
 * calling the method in the quickest time. This class should not be
 * instantiated itself, but subclasses should be used which overload
 * the methods as required.
 * <p>
 * Copyright (c) 2004 CERN, ITC-irst, PPARC, on behalf of the EU DataGrid.
 * For license conditions see LICENSE file or
 * <a href="http://www.edg.org/license.html">http://www.edg.org/license.html</a>
 * <p>
 * @since JDK1.4
 */
public class SkelOptor implements Optimisable {

    protected GridSite _site;
    protected NetworkClient _networkClient;

    /**
     * This constructor should only be called from SkelOptor
     * subclasses
     */
    protected SkelOptor( GridSite site) {

		_site = site;
		_networkClient =  new NetworkClient();
    }

    // TODO: factor out jbook from optimisers so the same constructor
    // can be used from CEs and the RB.
    protected SkelOptor() {
		_networkClient = new NetworkClient();
    }
 
    /**
     * A "standard" implementation of getBestFile. It simply looks at
     * the Replica Catalogue and current network status and returns the
     * replicas that will take the shortest time to access.
     */
    public DataFile[] getBestFile( String[] lfns, 
							      float[] fileFraction) { 
		DataFile files[] = new DataFile[lfns.length];
		DataFile replicas[];
		int i,j;
        NetworkCost minNC = null;
        ReplicaManager rm = ReplicaManager.getInstance();
		
		// For each requested LFN
		for(i=0;i<lfns.length;i++) {
		    
		    do {
				replicas = rm.listReplicas(lfns[i]);
				// Find the cheapest file:
				for(j=0;j<replicas.length;j++) {
					replicas[j].addPin(); // pin the file
					StorageElement remoteSE = replicas[j].se();
					    
					//If file has been deleted since listReplicas
					if( remoteSE==null){
						continue ;
					}
					    
					GridSite remoteSite = remoteSE.getGridSite();
					
					// If this is the same site, always use it.							    
					if( remoteSite == _site) {
						if(files[i]!=null)
							files[i].releasePin() ; // unpin previously selected file
						files[i] = replicas[j];
						break;
					}
					
					int transferSize = (int) (replicas[j].size() * fileFraction[i]);
					NetworkCost nc = _networkClient.getNetworkCosts( remoteSite, _site,  transferSize);
						
					if( (j == 0) || (nc.getCost() <= minNC.getCost())) {
						
						if( (j != 0) && (nc.getCost() == minNC.getCost())) {
							Random random = new Random();
							if( random.nextFloat() < 0.5) {
								minNC = nc;
								 if(files[i]!=null)
                                                        		files[i].releasePin() ;

		                                                files[i] = replicas[j];
							}
						}
						else {
							minNC = nc;
					         
							if(files[i]!=null)
								files[i].releasePin() ; // unpin previously selected file
							        
							files[i] = replicas[j];
						}
					}
					else { // replicas[j] not a good candidate,
					replicas[j].releasePin(); // unpin it
					}
				}
			} while( files[i] == null);
		}    
		return files;
	}

    /**
     * Calculate aggregated network costs for a single computing element.
     * Uses network costs and the Replica Catalog to find the best replica
     * of each file and sums the access costs.
     */
    public float getAccessCost(String[] lfns,
				ComputingElement ce,
				float[] fileFraction) {

		float aggregatedCost = 0;
		ReplicaManager rm = ReplicaManager.getInstance();
		float minCost = 0;

		GridSite ceGridSite = ce.getSite();

		for(int i=0; i<lfns.length; i++) {

	    	DataFile files[] = rm.listReplicas(lfns[i]);
	    	boolean minCostUninitialised = true;

	    	for(int j=0; j<files.length; j++) {

				if( files[j] == null)
		    		continue;
		    	
				StorageElement remoteSE = files[j].se();

				// the file may have been deleted since listReplicas was called
				if( remoteSE == null)
				    continue;

				GridSite seGridSite = remoteSE.getGridSite();
		    
				// if the file is on the local site stop looking for replicas
				if( ceGridSite == seGridSite) {
		    		minCostUninitialised = false;
		    		minCost = 0;
			    	break;
				}

				NetworkCost nc = _networkClient.getNetworkCosts( seGridSite, ceGridSite, 
								     														files[j].size());

				if( (nc.getCost() < minCost) || minCostUninitialised) {
			    	minCostUninitialised = false;
			    	minCost = nc.getCost();
				}
		    }

		    if( minCostUninitialised) {
				System.out.println( "optor> didn't find any replica for LFN "+lfns[i]);
				continue;
		    }

		    aggregatedCost += minCost;
		}

		return aggregatedCost;
    }


    public void initFilePrefetch(String[]  lfn,
				 ComputingElement ce)
    {
		System.out.println("initFilePrefetch");
    }

    
    public void cancelFilePrefetch(String[] lfn,
				   ComputingElement CE)
    {
		System.out.println("cancelFilePrefetch");
    }

}







