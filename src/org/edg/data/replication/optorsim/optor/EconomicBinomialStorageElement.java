package org.edg.data.replication.optorsim.optor;

import java.util.*;

import org.edg.data.replication.optorsim.infrastructure.DataFile;
import org.edg.data.replication.optorsim.infrastructure.GridSite;
import org.edg.data.replication.optorsim.infrastructure.OptorSimParameters;
import org.edg.data.replication.optorsim.infrastructure.MathSupport;

/**
 * A StorageElement which implements an economic model for file replication.
 * Predictions of future file values are based on a binomial distribution
 * of file indexes.
 * <p>
 * Copyright (c) 2002 CERN, ITC-irst, PPARC, on behalf of the EU DataGrid.
 * For license conditions see LICENSE file or
 * <a href="http://www.edg.org/license.html">http://www.edg.org/license.html</a>
 * <p>
 * @author caitrian
 * @since JDK1.4
 */
public class EconomicBinomialStorageElement 
					extends AccessHistoryStorageElement
					implements FileWorthStorageElement {

	private float _barF;   //parameters for binomial prediction function
	private float _S;
	private int _n = 5; 

	/**
	 * @param site The GridSite on which the SE is situated.
	 * @param capacity The total capacity of this SE.
	 */
	public EconomicBinomialStorageElement(GridSite site, long capacity) {
		super(site, capacity);
	}

    /**
     * Calculate the value of the file corresponding to <i>fileID</i>
     * using the binomial-based economic model.
     * @return The value of the file corresponding to <i>fileID</i>.
     */
	public double evaluateFileWorth( int fileId) {

		long dt = OptorSimParameters.getInstance().getDt();
		long dt1 = dt;

		//take the part of the _accessHistory to be considered in the evaluation
		TreeMap ref = getRecentAccessHistory(dt);

		//calculate evalfunction parameters as described in EvalDoc, section 3.1.1
		_barF = fileId;
		_S = 0;
		int r = ref.size();
        
		// see equation (9); here n is forced to have a minimum value (=5) fixed
		// empirically
		_n = 5;
		if (r > 5) 
			_n = r * (int)( dt1 / dt );

		//barF is caculated as mean value of recently requested file indexes
		Iterator it = ref.keySet().iterator();
		while (it.hasNext()){
			_barF += ( (DataFile)ref.get(it.next()) ).fileIndex();
		}

		_barF /= ( r + 1 );
					
		//see equation (10)
		it = ref.keySet().iterator();
		for (int j=0; it.hasNext(); j++) {
			int f_j = ( (DataFile)ref.get(it.next()) ).fileIndex();
			_S +=  ( f_j - _barF )*( f_j - _barF ) / ( r - j );
		} 
		if (_S == 0) {
		    // this should only happen if there have never been
		    // any local file accesses, so it's not really an error
		    // System.out.println( " Width S is incorrectly zero !!");
		}
		else {
			_S *= 2.0 / r;
		}
        
		//estimate value of the presently requested file
		double value = 0;          
		for(int j=1; j<=_n; j++) 
			value += MathSupport.symBinDistribution( (int) (j*_S), _barF, fileId);

		return value;
	}

	/**
	 * Returns the least valuable file on the SE, based on past
	 * access history. It does not consider the value of the
	 * file which may be replicated.
	 * @return The least valuable DataFile on the SE or null
	 * if none are deletable.
	 */
	public List filesToDelete(DataFile newFile) {

        List filesToDelete = new LinkedList();
        long deleteableFileSize = getAvailableSpace();

        // get the least valuable files until there is enough space
        do {
			DataFile chosenFile = null;

			// pick the file with least value from those in storage by
			// finding file with index furthest away from average
			for(Enumeration e = getAllFiles().elements(); e.hasMoreElements();) {

				DataFile file = (DataFile)e.nextElement();

				if(file.isDeleteable() && !filesToDelete.contains(file)) {
					if(chosenFile == null || 
							Math.abs(file.fileIndex()-_barF) 
							> Math.abs(chosenFile.fileIndex()-_barF)) {
					
						chosenFile = file;
					}
				}
			}
			if (chosenFile == null) {
				// this means there were no deleteable files left so perhaps
				// one was pinned during the operation
				System.out.println("Warning: couldn't delete enough files to replicate " +
					  newFile+" when it should have been possible. Have to use remote i/o");
				return null;
			}
			double value = evaluateFileWorth(chosenFile.fileIndex());
			chosenFile.setLastEstimatedValue(value);

            filesToDelete.add(chosenFile);
			deleteableFileSize += chosenFile.size();

		}  while(deleteableFileSize < newFile.size() );
		
		return filesToDelete;
	}

}
