package org.edg.data.replication.optorsim.optor;

import java.util.*;
 
import org.edg.data.replication.optorsim.infrastructure.*;

/**
 * This StorageElement implements a Zipf-based economic model for
 * file replication. Predictions of future file values are based
 * on a Zipf-like distribution of file indices.
 * <p>
 * Copyright (c) 2002 CERN, ITC-irst, PPARC, on behalf of the EU DataGrid.
 * For license conditions see LICENSE file or
 * <a href="http://www.edg.org/license.html">http://www.edg.org/license.html</a>
 * <p>
 * @author caitrian
 * @since JDK1.4
 */
public class EconomicZipfStorageElement 
						extends AccessHistoryStorageElement 
						implements FileWorthStorageElement {

	/**
	 * @param site The GridSite on which the SE is situated.
	 * @param capacity The total capacity of this SE.
	 */
	public EconomicZipfStorageElement(GridSite site, long capacity) {
		super(site, capacity);
	}

    /**
     * Calculate the value of the file corresponding to <i>fileID</i>
     * using the Zipf-based economic model.
     * @return The value of the file corresponding to <i>fileID</i>.
     */
	public double evaluateFileWorth(int fileId) {

		long dt = OptorSimParameters.getInstance().getDt();
		//take the part of the _accessHistory to be considered in the evaluation
		TreeMap ref = getRecentAccessHistoryIndices(dt);
		double value = BinaryTree.estimateFutureValueZipf( fileId, ref);
		return value;           
	}

    /**
     * Calls fileToDelete() to obtain the least valuable file currently
     * on the SE according to the Zipf-based economic model. This file
     * is returned unless the estimated value of the file with
     * <i>fileID</i> is less than the value of the least valuable
     * file.
     * @return The least valuable file on the SE or null if they are
     * all too valuable to delete.
     */
    public List filesToDelete(DataFile newFile) {

        DataFile chosenFile = null;
        long dt = OptorSimParameters.getInstance().getDt();
        List filesToDelete = new LinkedList();
        List nonAHFiles = new LinkedList();
        long deleteableFileSize = getAvailableSpace();

        //take the part of the _accessHistory to be considered in the evaluation
        Collection ref = getRecentAccessHistory(dt).values();

		// first find all the files not on the access history
		for( Enumeration e = getAllFiles().elements(); e.hasMoreElements();) {

			DataFile file = (DataFile)e.nextElement();
			if( file.isDeleteable() && !ref.contains(file)) {
				nonAHFiles.add(file);
			}
		}
        
		// pick random files until we have enough space or the list is empty
		while (!nonAHFiles.isEmpty()) {
			DataFile file = (DataFile)nonAHFiles.get((int)(Math.random()*nonAHFiles.size()));
			filesToDelete.add(file);
			nonAHFiles.remove(file);
			file.setLastEstimatedValue(evaluateFileWorth(file.fileIndex()));
			deleteableFileSize += file.size();
			if (deleteableFileSize >= newFile.size()) return filesToDelete;
		}

        BinaryTree t1 = new BinaryTree();
        BinaryTree t2 = new BinaryTree();

        for (Iterator it = ref.iterator(); it.hasNext();) {
            t1.getFromHistory(( (DataFile)it.next() ).fileIndex(),0,1);
        }

        t1.makeRanking(t2);
        int numNodes = t2.computeDistance(t1);

        do {
            int position1 = 0;
            int position2 = 0;
            for (Enumeration e = getAllFiles().elements(); e.hasMoreElements();) {

                DataFile file = (DataFile)e.nextElement();

                if (file.isDeleteable() && !filesToDelete.contains(file)) {

                    position2 = t1.searchPosition(file.fileIndex());
                    if (position2 == -1)
                        position2 = numNodes;

                    if (position2 > position1) {
                        chosenFile = file;
                        position1 = position2;
                    }
                }
            }
            if (filesToDelete.contains(chosenFile)) {
                // this means there were no deleteable files left so perhaps
                // one was pinned during the operation
                System.out.println("Warning: couldn't delete enough files to replicate " +
                    newFile+" when it should have been possible. Have to use remote i/o");
                return null;
            }
			double futurevalue = evaluateFileWorth(chosenFile.fileIndex());
			chosenFile.setLastEstimatedValue(futurevalue);
            filesToDelete.add(chosenFile);
            deleteableFileSize += chosenFile.size();

        } while(deleteableFileSize < newFile.size() );

    return filesToDelete;
    }

}
