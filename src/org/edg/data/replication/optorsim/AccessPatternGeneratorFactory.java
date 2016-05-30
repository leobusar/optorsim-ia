package org.edg.data.replication.optorsim;

import org.edg.data.replication.optorsim.infrastructure.OptorSimParameters;
import org.edg.data.replication.optorsim.infrastructure.GridJob;

/**
 * Used to obtain the type of Access Pattern Generator specified in 
 * the user parameters.
 * <p>
 * Copyright (c) 2004 CERN, ITC-irst, PPARC, on behalf of the EU DataGrid.
 * For license conditions see LICENSE file or
 * <a href="http://www.edg.org/license.html">http://www.edg.org/license.html</a>
 * <p>
 * @since JDK1.4
 */
abstract public class AccessPatternGeneratorFactory {

    /**
     * Returns an AccessPatternGenerator instance as defined by
     * the user parameters.
     */ 
    public static AccessPatternGenerator getAPGenerator(GridJob gridJob) {
	OptorSimParameters params = OptorSimParameters.getInstance();
	
	switch( params.getAccessPatternGenerator()) {
	    
	case SEQUENTIAL_ACCESS_GENERATOR:
	    return new SequentialAccessGenerator(gridJob, 
						 gridJob.getFileFraction());
/*	case SEQUENTIAL_RANDOM_ENTRY_ACCESS_GENERATOR:
		return new SequentialRandomEntryAccessGenerator(gridJob,
						 gridJob.getFileFraction());	    */
	case RANDOM_ACCESS_GENERATOR:
	    return new RandomAccessGenerator(gridJob,
					     gridJob.getFileFraction());
	    
	case RANDOM_WALK_UNITARY_ACCESS_GENERATOR:
	    return new RandomWalkUnitaryAccessGenerator(gridJob, 
							gridJob.getFileFraction());
	    
	case RANDOM_WALK_GAUSSIAN_ACCESS_GENERATOR:
	    return new RandomWalkGaussianAccessGenerator(gridJob,
							 gridJob.getFileFraction());
	    
	case RANDOM_ZIPF_ACCESS_GENERATOR:
	    return new RandomZipfAccessGenerator(gridJob,
						 gridJob.getFileFraction());
	    
	default:
	    System.out.println("You have picked a non-existent access generator, please try again.");
	    System.exit(1);
	    //break;
	}
	return null;
    }

    /**
     * Returns a String giving the type of access patterns used.
     */
    public static String getAccessPatternName() {

	String accessPattern = new String();
	OptorSimParameters params = OptorSimParameters.getInstance();

	switch (params.getAccessPatternGenerator()) {
	case SEQUENTIAL_ACCESS_GENERATOR:
	    accessPattern = "Sequential";
	    break;
/*	case SEQUENTIAL_RANDOM_ENTRY_ACCESS_GENERATOR:
		accessPattern = "Sequential Random Entry";
		break;*/
	case RANDOM_ACCESS_GENERATOR:
	    accessPattern = "Random";
	    break;
	case RANDOM_WALK_UNITARY_ACCESS_GENERATOR:
	    accessPattern = "Unitary Random Walk";
	    break;
	case RANDOM_WALK_GAUSSIAN_ACCESS_GENERATOR:
	    accessPattern = "Gaussian Random Walk";
	    break;
	case RANDOM_ZIPF_ACCESS_GENERATOR:
	    accessPattern = "Zipf Distribution";
	    break;
	default:
	    System.out.println("OptorInformation>> Error: Invalid access pattern");
	    System.exit(1);
	    break;
	}

	return accessPattern;
    }

    private static final int SEQUENTIAL_ACCESS_GENERATOR = 1;
 //   private static final int SEQUENTIAL_RANDOM_ENTRY_ACCESS_GENERATOR = 2;
    private static final int RANDOM_ACCESS_GENERATOR = 2;
    private static final int RANDOM_WALK_UNITARY_ACCESS_GENERATOR = 3;
    private static final int RANDOM_WALK_GAUSSIAN_ACCESS_GENERATOR = 4;
    private static final int RANDOM_ZIPF_ACCESS_GENERATOR = 5;
}
