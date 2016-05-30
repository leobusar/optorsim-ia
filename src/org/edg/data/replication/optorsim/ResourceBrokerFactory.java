package org.edg.data.replication.optorsim;

import org.edg.data.replication.optorsim.infrastructure.OptorSimParameters;

/**
 * Factory method for generating a ResourceBroker, based on user requirements.  This
 * also enforces the singleton nature of the RB.
 * <p>
 * Copyright (c) 2004 CERN, ITC-irst, PPARC, on behalf of the EU DataGrid.
 * For license conditions see LICENSE file or
 * <a href="http://www.edg.org/license.html">http://www.edg.org/license.html</a>
 * <p>
 * @since JDK1.4
 * @author paulm
 */
public class ResourceBrokerFactory {
	
	private static ResourceBroker _instance = null;
	
	/**
	 * Get the instance of the ResourceBroker.  If one doesn't exist, a new one is created.
	 * @return the active resourceBroker
	 */
	public static ResourceBroker getInstance() {	
		if( _instance == null)
			_instance = instantiateRB();
		
		return _instance;		
	}
	
	private static ResourceBroker instantiateRB() {
		OptorSimParameters params = OptorSimParameters.getInstance();
		
		// run until all the jobs have been submitted
		int scheduler = params.getScheduler();
		
		switch( scheduler) {
			case 1:
				System.out.println( "ResourceBrokerFactory> creating a RandomCEResourceBroker");
				return new RandomCEResourceBroker();
			case 2:
				System.out.println("ResourceBrokerFactory> creating a QueueLengthResourceBroker");
				return new QueueLengthResourceBroker();
			case 3:
				System.out.println("ResourceBrokerFactory> creating an AccessCostResourceBroker");
				return new AccessCostResourceBroker();
			case 4:
				System.out.println( "ResourceBrokerFactory> creating a CombinedCostResourceBroker");
				return new CombinedCostResourceBroker();
		}
		
		System.out.println( "ResourceBrokerFactory> unknown ResourceBroker type: "+scheduler);
		System.exit(1);
		
		return null;

	}
	
}
