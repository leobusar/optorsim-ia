package org.edg.data.replication.optorsim.optor;

import org.edg.data.replication.optorsim.infrastructure.OptorSimParameters;
import org.edg.data.replication.optorsim.infrastructure.GridSite;

/**
 * This factory is used by Computing Elements to obtain an instance
 * of the required optimiser.
 * <p>
 * Copyright (c) 2004 CERN, ITC-irst, PPARC, on behalf of the EU DataGrid.
 * For license conditions see LICENSE file or
 * <a href="http://www.edg.org/license.html">http://www.edg.org/license.html</a>
 * <p>
 * @since JDK1.4
 */
abstract public class OptimiserFactory {
    
    private static final int SIMPLE_OPTIMISER = 1;
    private static final int LRU_OPTIMISER = 2;
    private static final int LFU_OPTIMISER = 3;
    private static final int ECO_MODEL_OPTIMISER = 4;
    private static final int ECO_MODEL_OPTIMISER_ZIPF_BASED = 5;

    /**
     * Returns an Optimisable instance based on the parameters input
     * to the simulation. This method is called by Computing Elements
     * which are on <em>site</em>.
     * @return An Optimisable instance.
     */
    public static Optimisable getOptimisable( GridSite site) {

	OptorSimParameters params = OptorSimParameters.getInstance();
		
	switch( params.getOptimiser()) {
		
	case SIMPLE_OPTIMISER:
	    return new SimpleOptimiser(site);
	    
	case LRU_OPTIMISER:
	    return new LruOptimiser(site);
	    
	case LFU_OPTIMISER:
	    return new LfuOptimiser(site);
	    
	case ECO_MODEL_OPTIMISER:
	    return new EcoBinModelOptimiser(site);
	    
	case ECO_MODEL_OPTIMISER_ZIPF_BASED:
	    return new EcoZipfModelOptimiser(site);

	default:
	    System.out.println("You have picked a non-existent optimiser, please try again.");
	    System.exit(1);
	}
	
	return null;  // Code never gets here, but it shuts up the return-checker 
    }
    
    /**
     * Returns an Optimisable instance based on the parameters input
     * to the simulation. This method is used by the RB which does
     * not reside on any particular site and only uses getAccessCost().
     * @return An Optimisable instance.
     */
    public static Optimisable getOptimisable() {

	return new SimpleOptimiser();
    }

    /**
     * Simple routine that returns a text string describing the optimisation algorithm
     */
    public static String getOptAlgo() {
	
	OptorSimParameters params = OptorSimParameters.getInstance();
	
	switch (params.getOptimiser()) {
	case  SIMPLE_OPTIMISER:
	    return "No Replication";
	case LRU_OPTIMISER:
	    return "Always Replicate, Delete Oldest File";
	case LFU_OPTIMISER:
	    return "Always Replicate, Delete Least Accessed File";
	case ECO_MODEL_OPTIMISER:
	    return "Economic Model, Binomial Prediction Function";
	case ECO_MODEL_OPTIMISER_ZIPF_BASED:
	    return "Economic Model, Zipf Prediction Function";
	}
	
	return "Unknown optimiser:"+params.getOptimiser();
    }

}
