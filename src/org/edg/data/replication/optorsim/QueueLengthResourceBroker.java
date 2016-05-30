package org.edg.data.replication.optorsim;

import org.edg.data.replication.optorsim.infrastructure.ComputingElement;
import org.edg.data.replication.optorsim.infrastructure.GridContainer;
import org.edg.data.replication.optorsim.infrastructure.GridJob;
import org.edg.data.replication.optorsim.infrastructure.OptorSimParameters;
import org.edg.data.replication.optorsim.optor.Optimisable;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

/**
 * Selects the Computing Elements on
 * which to run jobs according to the queue length at each one.
 * <p>
 * Copyright (c) 2004 CERN, ITC-irst, PPARC, on behalf of the EU DataGrid.
 * For license conditions see LICENSE file or
 * <a href="http://www.edg.org/license.html">http://www.edg.org/license.html</a>
 * <p>
 * @since JDK1.4
 *  @author caitrian
 */

public class QueueLengthResourceBroker extends SkelResourceBroker {

 
	private static Random _random;
	private static boolean _randomSeed = OptorSimParameters.getInstance().useRandomSeed();

	static {
		if(_randomSeed) _random = new Random();
		else _random = new Random(999);
	}

    /**
     * Returns the CE with the smallest job queue that will run <i>job</i>.
     */
	public ComputingElement findCE(GridJob job, Optimisable optor) {

		GridContainer gc = GridContainer.getInstance();
		int shortestQueue=Integer.MAX_VALUE;		
		List lowestQ = new LinkedList();
			
		for( Iterator iCE = gc.freeCEs(); iCE.hasNext();) {					
				
		    ComputingElement ce = (ComputingElement) iCE.next();
		    System.out.println(ce.toString());
		    if( !ce.getSite().acceptsJob( job)) 
		    	continue;
		   
		    
		    JobHandler jhandler = ce.getJobHandler();					
		    if( jhandler.isFull()) 
		    	continue;
		   
		    
		    int queueSize = jhandler.getQueueSize();
		    if( queueSize < shortestQueue)  {
				shortestQueue = queueSize;
				lowestQ.clear();
				lowestQ.add(ce);
		    }
		    else if (queueSize == shortestQueue){
		    	lowestQ.add(ce);
		    }
		}
		if (lowestQ.isEmpty()) return null;
		return (ComputingElement) lowestQ.get((int)Math.floor(_random.nextInt(lowestQ.size())));
	}
}
