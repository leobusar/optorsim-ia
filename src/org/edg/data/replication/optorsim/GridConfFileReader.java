package org.edg.data.replication.optorsim;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.edg.data.replication.optorsim.auctions.AccessMediator;
import org.edg.data.replication.optorsim.infrastructure.GridContainer;
import org.edg.data.replication.optorsim.infrastructure.GridSite;
import org.edg.data.replication.optorsim.infrastructure.OptorSimParameters;
import org.edg.data.replication.optorsim.infrastructure.StorageElement;
import org.edg.data.replication.optorsim.optor.StorageElementFactory;

/**
 * This class provides a method to read the grid configuration 
 * file and build a route map, for the site to site connections.  Other
 * methods are provided to return the reconstructed information.
 * <p>
 * Copyright (c) 2002 CERN, ITC-irst, PPARC, on behalf of the EU DataGrid.
 * For license conditions see LICENSE file or
 * <a href="http://www.edg.org/license.html">http://www.edg.org/license.html</a>
 * <p>
 * @since JDK1.4
 */

// TODO: class need major cleanup and (perhaps) rewrite.
// TODO: it would be nice to move this to infrastructure but it
// creates specific instances of CEs and SEs so it's difficult.
// We could get this to read in the info, then another class 
// creates the objects.
 
public class GridConfFileReader{

    private Vector _gridSites = new Vector();
    private Vector _ceFlag = new Vector();
    private Vector _seFlag = new Vector();
    private Vector _ceCapacity = new Vector(); // AV Computation capacity of WN
    private Vector _networkMetricStatic = new Vector();
    private Hashtable _routeHash = new Hashtable();

    private String _filename;
    private int _numSites;

    private static GridConfFileReader _gridConfFileReaderInstance = null;

    /**
     * 	 * Returns an instance of the GridConfFileReader, instantiating it if 
     * 	 	 * called for the first time and returning the existing instance otherwise.
     * 	 	 	 */
    public static GridConfFileReader getInstance() {
        if( _gridConfFileReaderInstance == null)
            _gridConfFileReaderInstance = new GridConfFileReader();

        return _gridConfFileReaderInstance;
    }

    private GridConfFileReader() {
        OptorSimParameters params = OptorSimParameters.getInstance();
        GridContainer gc = GridContainer.getInstance();

        _filename = params.getGridConfigfile();

        System.out.println("GridConfFileReader> reading file "+_filename);
        read();
        createGridSites();

        findRoutes();
        associateNeighbours();		
    }


    /**
     * Routine to assign GridSites, with Computing and Storage Elements based on the Grid
     * configuration file.
     *
     */    
    public void createGridSites(){
    	int i, j,  numSE, workerNodes;
        float wnCapacity ;
       
        OptorSimParameters params = OptorSimParameters.getInstance();
        GridContainer gc = GridContainer.getInstance();
        StorageElementFactory seFactory = StorageElementFactory.getInstance();

        for(i=0;i< _numSites; i++) {

            GridSite gsite = new GridSite();

            workerNodes = ((Integer)_ceFlag.get(i)).intValue();
            wnCapacity = ((Float)_ceCapacity.get(i)).floatValue();
            numSE = ((Long)((Vector)_seFlag.get(i)).get(0)).intValue();

            if(workerNodes > 0) {
                if (params.getComputingElement()==1)
                    new SimpleComputingElement( gsite, workerNodes, wnCapacity);
                if (params.getComputingElement() == 2)
                    new BatchComputingElement(gsite, workerNodes, wnCapacity);
                if(params.auctionOn()) AccessMediator.addAM(gsite);
            }
            for (j=1;j<=numSE;j++) {
                long size = ((Long)((Vector)_seFlag.get(i)).get(j)).longValue();
                StorageElement se = seFactory.getStorageElement( gsite, size);
            }

            gc.addSite( gsite);

            _gridSites.add( gsite);
        }		

    }



    /**
     * A method to read the grid configuration file.
     */
    private void read() {
        String inputLine,tmp;
        int j=0, noSE=0;
        int i=0,offset,oldoffset,linelength;
        FileReader read=null;
        
        try{
            read = new FileReader( _filename);
        }
        catch(FileNotFoundException e) {
            System.out.println("\n ERROR::GridConfFileReader> File "
                    +_filename+" not found.\n");
            System.exit(1);
        }

        BufferedReader in = new BufferedReader( read);		
        try{
            while( (inputLine = in.readLine()) != null) {

                inputLine=inputLine.trim();

                if( inputLine.startsWith("#"))  // skip lines starting with a #
                    continue;

                Vector seCapacity = new Vector();

                i=0;
                oldoffset=0;
                linelength = inputLine.length();
                offset=inputLine.indexOf(" ");
                
                while(offset<linelength) {
                    if(offset==(linelength-1)) {
                        tmp=inputLine.substring(oldoffset);
                    } else {
                        tmp=inputLine.substring(oldoffset,offset);
                    }

                    if(i==0) {
                        _ceFlag.add(new Integer(Integer.parseInt(tmp)));
                    } else if(i==1){ // capacity of CEs
                        _ceCapacity.add(new Float(Float.parseFloat(tmp))) ;
                    } else if(i==2) { // number of SE
                        noSE = Integer.parseInt(tmp);
                        seCapacity.add(new Long( noSE));
                        if(noSE==0)
                            _seFlag.add(seCapacity);
                    } else if(i<noSE+3) { // capacity of SEs
                        Double tmpdouble = new Double((Long.parseLong(tmp)));
                        seCapacity.add(new Long(tmpdouble.longValue()));
                        if(i==noSE+2) // the capacity of the last SE
                            _seFlag.add(seCapacity);
                    } else { // network bandwith matrix
                    	_networkMetricStatic.add(new Float(Float.parseFloat(tmp)));
                    }
                    oldoffset=offset;
                    oldoffset++;
                    if(oldoffset<linelength) {
                        offset=inputLine.indexOf(" ",oldoffset);
                        if(offset==-1)
                            offset=linelength-1;
                    } else {
                        offset=linelength;
                    }
                    i++;
                }
                j++;
            }
            in.close();
            read.close();
        }
        catch( Exception e) {
            System.out.println("Exception: "+e.getMessage()+ " whilst using file");
            System.exit(1);
        }

        i-=2; // Just matrix dimension

        _numSites=j;
    }

    /**
     * Tell all the GridSites to ask about neighbouring sites. This has to be done
     * after the network routes are established
     */
    private void associateNeighbours() {
        GridContainer gc = GridContainer.getInstance();

        // For each grid site, find the predecessor Map and publish it in the GridContainer
        for( Enumeration eSite = _gridSites.elements(); eSite.hasMoreElements();) {
            GridSite site = (GridSite) eSite.nextElement();
            site.associateNeighbours();
        }
    }


    /**
     * Routine to find the best route from all GridSites to all other GridSites and store this information
     * in the GridContainer.
     */
    private void findRoutes() {
        GridContainer gc = GridContainer.getInstance();
        Map neighbours = buildNeighbours();
        
        // For each grid site, find the predecessor Map and publish it in the GridContainer
        for( Iterator iSite = gc.iterateGridSites(); iSite.hasNext();) {
        	GridSite site = (GridSite) iSite.next();
        	Map pred = dijkstraSearch( site, neighbours);
            gc.addNetworkRoutes( site, pred, (Map) neighbours.get( site));
        }
    }

    /**
     * Search for the shortest routes for a site to all other sites using
     * Dijkstra's Algorithm. (see http://ciips.ee.uwa.edu.au/~morris/Year2/PLDS210/dijkstra.html)
     * This quickly finds the shortest route from a selected site to all other sites. 
     */
    private Map dijkstraSearch( GridSite sourceSite, Map neighbours) {
    	Set solvedSites = new HashSet();
        Set unsolvedSites = new HashSet( _gridSites);
        Map currentEstimate = new HashMap();   // current est. of minimum cost for traversing from source Site.
        Map predecessor = new HashMap();  // optimum route is stored as multiple hops back from node to pred(node)

        // Initalise costs
        for( Enumeration eSite =_gridSites.elements();  eSite.hasMoreElements() ;) {
            GridSite site = (GridSite) eSite.nextElement();
            float value = (site == sourceSite) ? Float.POSITIVE_INFINITY : 0;
            currentEstimate.put( site, new Float( value));
        }
        while( !unsolvedSites.isEmpty()) {
            // Choose the site with the lowest cost in the unsolved set
            GridSite site = extractHighestBandwidth( unsolvedSites, currentEstimate);
      
            // For each neighbour, "relax", ie check if routing via neighbour is better
            Map siteNeighbours = (Map) neighbours.get( site);
            for( Iterator iNSite = siteNeighbours.keySet().iterator(); iNSite.hasNext();) {
                GridSite nSite = (GridSite) iNSite.next();
                float bw = ((Float)siteNeighbours.get(nSite)).floatValue();
                relax( site, nSite, sourceSite, bw, currentEstimate, predecessor);  			
            }

            // Add this site to the list of ones we've done. 
            solvedSites.add( site);	
        }

        return predecessor;
    }


    /**
     * Method for doing a Dijkstra "relax" step.  Basically looks to see if routing via the primary site
     * pSite is better for the neighbouring site nSite.
     * @param pSite The primary site
     * @param nSite The neighbouring site
     * @param sourceSite The source site for the Dijkstra calculation
     * @param bw The link bw for going from primary site to neighbouring site
     * @param estimate The current map of bw estimates
     * @param predecessor The map of predecessor. This maps a GridSite to a GridSite, indicating the optimal route.
     */
    private void relax( GridSite pSite, GridSite nSite, GridSite sourceSite, float bw, Map estimate, Map predecessor) {
    	float estimatePSite = ((Float)estimate.get(pSite)).floatValue();
        float estimateNSite = ((Float)estimate.get(nSite)).floatValue();
   
        // The effective BW is always the lower of the two.
        float effectiveBW = (estimatePSite < bw) ? estimatePSite : bw;
    
        if( effectiveBW > estimateNSite) {
            estimate.put( nSite, new Float( effectiveBW));
            predecessor.put( nSite, pSite);			
        }

        // For two possible routes of equal minimum bandwidth, pick the one with fewest hops
        if( effectiveBW == estimateNSite){
        	getMinimumHopRoute(pSite, nSite, sourceSite, predecessor);			
        }
    }

    /**
     * Method for checking which of two possible routes in a Dijktsra calculation has 
     * the fewest number of hops back to the source site.
     * @param pSite The primary site
     * @param nSite The neighbouring site under consideration
     * @param sourceSite The source site for this Dijkstra calculation
     * @param predecessor The map of predecessors. This maps a GridSite to a GridSite,  indicating the optimal route.
     */
    private void getMinimumHopRoute(GridSite pSite, GridSite nSite, GridSite sourceSite, Map predecessor) {
        GridSite nextSite = new GridSite();
        int oldHops = 0;
        //get number of hops for route already in predecessor map
        for( GridSite site = nSite; site != sourceSite;site = (GridSite)predecessor.get(site)){
        	oldHops++;
        }
        int newHops = 1;
        //get number of hops for new route under consideration
        for( GridSite site = pSite; site != sourceSite;site = (GridSite)predecessor.get(site))
            newHops++;

        //if new route shorter than old, put it in the predecessor map
        if( newHops < oldHops) {
            predecessor.put( nSite, pSite);
        }
    }

    /**
     * Search for the highest bandwidth site in a given set.  It also removes this site
     * from the Set
     * @param sites The set of sites to consider
     * @param costs A Map that maps from GridSite to Float bandwidth.
     * @return A GridSite with the highest bandwidth, or null
     */
    private GridSite extractHighestBandwidth( Set sites, Map costs) {
        GridSite bestSite = null;
        boolean firstTime = true;
        float maxBW = 0; // Variable doesn't need to be initialised, but it keeps Eclipse happy.

        for( Iterator iSite = sites.iterator(); iSite.hasNext();) {
            GridSite candidate = (GridSite) iSite.next();
            float bw = ((Float) costs.get( candidate)).floatValue();

            if( firstTime || (bw > maxBW)) {
                bestSite = candidate;
                maxBW = bw;
                firstTime = false;
            }
        }

        if( bestSite != null) 
            sites.remove( bestSite);

        return bestSite;		
    }


    /**
     * Look in connectivity matrix and produce a Map of neighbours.  This map takes a GridSite as
     * the key and returns a further Map.  This second Map contains all the neighbouring sites and their
     * associated costs.
     * @return A map describing network costs between sites
     */
    public Map buildNeighbours() {

        Map neighbours = new HashMap();

        for(int i=0;i<_numSites;i++) {

            GridSite siteA = getSiteByID( i);
            Map costs = new HashMap();

            for(int j=0;j<_numSites;j++) {

                if( i == j)
                    continue; //skip siteA --> siteA

                float cost = ((Float)_networkMetricStatic.get(i+j*_numSites)).floatValue();

                if( cost == 0)
                    continue;  // skip siteA --> siteB if they're not connected

                costs.put(  getSiteByID(j), new Float( cost));				
            }
            neighbours.put( siteA, costs);
        }

        return neighbours;
    }


    private GridSite getSiteByID( int id) {
        return (GridSite) _gridSites.get(id);
    }

}
