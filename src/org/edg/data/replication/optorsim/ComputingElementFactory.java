package org.edg.data.replication.optorsim;

import org.edg.data.replication.optorsim.infrastructure.ComputingElement;
import org.edg.data.replication.optorsim.infrastructure.OptorSimParameters;
import org.edg.data.replication.optorsim.infrastructure.GridSite;

/**
 * @author caitrian
 * Factory method for generating correct type of ComputingElement
 * Copyright (c) 2005 CERN, ITC-irst, PPARC, on behalf of the EU DataGrid.
 * For license conditions see LICENSE file or
 * <a href="http://www.edg.org/license.html">http://www.edg.org/license.html</a>
 * <p>
 * @since JDK1.4
 */
public class ComputingElementFactory {
	
	private static ComputingElementFactory _instance = null;
	
	private static final int SIMPLE_COMPUTING_ELEMENT = 1;
	private static final int BATCH_COMPUTING_ELEMENT = 2;
	
	/**
	 * Returns the singleton instance of the ComputingElementFactory, 
	 * creating it if it has not already been instantiated.
	 * @return The ComputingElementFactory instance.
	 */
	public static ComputingElementFactory getInstance() {
		
		if( _instance == null) {
			_instance = new ComputingElementFactory();
		}
		return _instance;
	}
	
	public ComputingElement getComputingElement(GridSite site, int wn, long capacity){
		
		OptorSimParameters params = OptorSimParameters.getInstance();
		
		switch( params.getComputingElement()) {
			case SIMPLE_COMPUTING_ELEMENT: 
				return new SimpleComputingElement(site, wn, capacity);
			case BATCH_COMPUTING_ELEMENT:
				return new BatchComputingElement(site, wn, capacity);
		}
		return null;
	}
	
}
