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
 * This class is an implementation of the ResourceBroker which selects sites on
 * which to run jobs according to the "queue access cost" -estimated time to get all
 * files for the current job + to get all files for all jobs in the queue at the CE in question. 
 * <p>
 * Copyright (c) 2004 CERN, ITC-irst, PPARC, on behalf of the EU DataGrid.
 * For license conditions see LICENSE file or
 * <a href="http://www.edg.org/license.html">http://www.edg.org/license.html</a>
 * <p>
 * @since JDK1.4
 */

public class CombinedCostResourceBroker extends SkelResourceBroker
{

	private static Random _random;
	private static boolean _randomSeed = OptorSimParameters.getInstance().useRandomSeed();

	static {
		if(_randomSeed) _random = new Random();
		else _random = new Random(999);
	}

    /**
     * Returns the CE which has the lowest queue access cost.
     */
	public ComputingElement findCE( GridJob job, Optimisable optor ) {
		long start_time = System.currentTimeMillis();
		GridContainer gc = GridContainer.getInstance();
		double bestCost=Double.MAX_VALUE;
		List lowestAC = new LinkedList();
		
		String[] lfns= new String[job.size()];
		float[] fileFraction = new float [job.size()];
		
		job.toArray( lfns);
		for( int i=0; i < job.size(); i++)
			fileFraction [i] = (float)1.0;
			
		for( Iterator iCE = gc.freeCEs(); iCE.hasNext();) {
			ComputingElement ce = (ComputingElement) iCE.next();				
		    if( !ce.getSite().acceptsJob( job))
				continue;
				
		    JobHandler jhandler = ce.getJobHandler();					
		    if( jhandler.isFull())
				continue;
			
		    double qAccessCost = jhandler.getQueueAccessCost( optor, ce);	
		    double accessCost = optor.getAccessCost( lfns, ce, fileFraction);
		    double totalCost = qAccessCost + accessCost;

		    if( totalCost < bestCost)  {
				bestCost = totalCost;
				lowestAC.clear();
				lowestAC.add(ce);
			}
			else if (accessCost == bestCost){
				lowestAC.add(ce);
			}
		}
		if (lowestAC.isEmpty()) return null;
		return (ComputingElement) lowestAC.get((int)Math.floor(_random.nextInt(lowestAC.size())));
	}

}
