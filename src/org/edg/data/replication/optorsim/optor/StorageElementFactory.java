package org.edg.data.replication.optorsim.optor;

import org.edg.data.replication.optorsim.infrastructure.GridSite;
import org.edg.data.replication.optorsim.infrastructure.OptorSimParameters;
import org.edg.data.replication.optorsim.infrastructure.StorageElement;

/**
 * Factory method for generating the correct type of StorageElement for the
 *  given optimiser.
 * <p>
 * Copyright (c) 2004 CERN, ITC-irst, PPARC, on behalf of the EU DataGrid.
 * For license conditions see LICENSE file or
 * <a href="http://www.edg.org/license.html">http://www.edg.org/license.html</a>
 * <p>
 * @since JDK1.4
 * @author caitrian
 */
public class StorageElementFactory {

	private static StorageElementFactory _instance = null;
	
	private static final int SIMPLE_OPTIMISER = 1;
	private static final int LRU_OPTIMISER = 2;
	private static final int LFU_OPTIMISER = 3;
	private static final int ECO_MODEL_OPTIMISER = 4;
	private static final int ECO_MODEL_OPTIMISER_ZIPF_BASED = 5;
	
	/**
	 * Returns the singleton instance of the StorageElementFactory, 
	 * creating it if it has not already been instantiated.
	 * @return The StorageElementFactory instance.
	 */
	public static StorageElementFactory getInstance() {
		
		if( _instance == null) {
			_instance = new StorageElementFactory();
		}
		return _instance;
	}

	/**
	 * Generates a new StorageElement according to the optimiser
	 * chosen in the parameters file.
	 * @param site The GridSite on which the SE should be created.
	 * @param capacity The capacity of the SE.
	 * @return A new StorageElement object.
	 */
	public StorageElement getStorageElement( GridSite site, long capacity) {
		
		OptorSimParameters params = OptorSimParameters.getInstance();
		
		switch( params.getOptimiser()) {
		
				case SIMPLE_OPTIMISER:
					return  new SimpleStorageElement( site, capacity);

				case LRU_OPTIMISER:
					return  new LruStorageElement( site, capacity);

				case LFU_OPTIMISER:
					return new  LfuStorageElement( site, capacity);

				 case ECO_MODEL_OPTIMISER:
					return new  EconomicBinomialStorageElement( site, capacity);

				case ECO_MODEL_OPTIMISER_ZIPF_BASED:
					return  new EconomicZipfStorageElement( site, capacity);
				 
				 default:
					System.out.println("You have picked a non-existent optimiser, please try again.");
					System.exit(1);
				 }
				
		return null;  // Code never gets here, but it shuts up the return-checker 
	}

}
