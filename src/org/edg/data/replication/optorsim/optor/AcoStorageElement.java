package org.edg.data.replication.optorsim.optor;

import java.util.*;

import org.edg.data.replication.optorsim.infrastructure.*;
import org.edg.data.replication.optorsim.reptorsim.NetworkClient;
import org.edg.data.replication.optorsim.reptorsim.NetworkCost;
import org.edg.data.replication.optorsim.reptorsim.ReplicaManager;

/**
 * This StorageElement implements a Zipf-based economic model for file
 * replication. Predictions of future file values are based on a Zipf-like
 * distribution of file indices.
 * <p>
 * Copyright (c) 2002 CERN, ITC-irst, PPARC, on behalf of the EU DataGrid. For
 * license conditions see LICENSE file or
 * <a href="http://www.edg.org/license.html">http://www.edg.org/license.html</a>
 * <p>
 * @author caitrian
 * @since JDK1.4
 */
public class AcoStorageElement
        extends AccessHistoryStorageElement {

    /**
     * @param site The GridSite on which the SE is situated.
     * @param capacity The total capacity of this SE.
     */
    public AcoStorageElement(GridSite site, long capacity) {
        super(site, capacity);
    }

    /**
     * Calculate the value of the file corresponding using the history worth +
     * storage cost + transfer cost model.
     *
     * @param file The file to be evaluated.
     * @return The value of the file corresponding to <i>fileID</i>.
     */
    public double evaluateFileWorth(DataFile file) {

        // get storage cost of file in the current SE.
        //StorageElement se = file.se();
        double scost = ((double) file.size()) / ((double)file.size() + getAvailableSpace());

        /**
         * *Calculate history worth ***
         */
        long dt = OptorSimParameters.getInstance().getDt();
        double hworthWeight = 1;
        double tcostWeight = 0.07;
        double scostWeight = 0.25;


        //take the part of the _accessHistory to be considered in the evaluation
        Map recentHistory = getRecentAccessHistory(dt);
        Map fileCount = new HashMap();
        int totalAccess = 0; 
        int fileAccess, numFiles=0;

        for (Iterator i = recentHistory.values().iterator(); i.hasNext();) {

            // sort history into a map of filenames to their no. of accesses
            String historyFile = ((DataFile) i.next()).lfn();

            if (fileCount.containsKey(historyFile)) {
                int count = ((Integer) fileCount.get(historyFile)).intValue();
                fileCount.put(historyFile, new Integer(++count));
            } else {
                fileCount.put(historyFile, new Integer(1));
                numFiles++;
            }
            totalAccess++;
        }
        if (fileCount.containsKey(file.lfn())) {
            fileAccess = ((Integer) fileCount.get(file.lfn())).intValue();
        } else {
            // Un valor por defecto aleatorio entre 0 y 10 para que no sea descartado por uso.
            fileAccess = 1;//(int) Math.round(10*Math.random()); 
        }
        double hworth = 1.0;
        if (totalAccess!=0)
            hworth = ((double) fileAccess) / totalAccess;

        /**
         * *Calculate transfer cost ***
         */
        double tcost = getAccessCost(file.lfn());

        System.out.println("**** WHWORTH "+ hworthWeight*hworth + " WTCOST " +tcostWeight*(0.02*tcost) +" WSCOST: "+ scostWeight*scost); 
        double value = hworthWeight * hworth + tcostWeight * (0.02) * tcost - scostWeight * scost;

        return value;
    }

    /**
     * Calls fileToDelete() to obtain the least valuable file currently on the
     * SE according to the Zipf-based economic model. This file is returned
     * unless the estimated value of the file with
     * <i>fileID</i> is less than the value of the least valuable file.
     *
     * @param newFile
     * @return The least valuable file on the SE or null if they are all too
     * valuable to delete.
     */
    
    public List filesToDelete(DataFile newFile) {
        DataFile chosenFile = null;
        Map fileCount = getAllFiles();
        List filesToDelete = new LinkedList();
        List nonAHFiles = new LinkedList();
        long deleteableFileSize = getAvailableSpace();

        // if this didn't yield enough space take files from the list
        do {
            double minCount = Double.MAX_VALUE;
//            for (Iterator i = fileCount.keySet().iterator(); i.hasNext();) {
	    for( Enumeration i = getAllFiles().elements(); i.hasMoreElements();) {

                DataFile file = (DataFile)i.nextElement();
                
                if (file != null && file.isDeleteable()) {
                    double worth=evaluateFileWorth(file);
                    file.setLastEstimatedValue(worth);
                    if (worth < minCount) {
                        chosenFile = file;
                        minCount = worth;
                    }
                }
            }
                filesToDelete.add(chosenFile);
                deleteableFileSize += chosenFile.size();
            if (fileCount.remove(chosenFile.lfn()) == null) {
                // this means there were no deleteable files left so perhaps
                // one was pinned during the operation
                System.out.println("Warning: couldn't delete enough files to replicate "
                        + newFile + " when it should have been possible. Have to use remote i/o");
                return null;
            }
            
        } while (deleteableFileSize < newFile.size());

        return filesToDelete;
    }

    /**
     * Calculate aggregated network costs for a single storage element. Uses
     * network costs and the Replica Catalog to find the best replica of each
     * file and sums the access costs.
     */
    public float getAccessCost(String lfns) {

        float aggregatedCost = 0;
        ReplicaManager rm = ReplicaManager.getInstance();
        float minCost = 0;

        GridSite ceGridSite = this.getGridSite();

//        for (int i = 0; i < lfns.length; i++) {
        DataFile files[] = rm.listReplicas(lfns);
        boolean minCostUninitialised = true;

        for (int j = 0; j < files.length; j++) {

            if (files[j] == null) {
                continue;
            }

            StorageElement remoteSE = files[j].se();

            // the file may have been deleted since listReplicas was called
            if (remoteSE == null) {
                continue;
            }

            GridSite seGridSite = remoteSE.getGridSite();

            // if the file is on the local site continue looking for replicas
            if (ceGridSite == seGridSite) {
//                    minCostUninitialised = false;
//                    minCost = 0;
//                    break;
                continue;
            }

            NetworkClient networkClient = new NetworkClient();

            NetworkCost nc = networkClient.getNetworkCosts(seGridSite, ceGridSite,
                    files[j].size());

            if ((nc.getCost() < minCost) || minCostUninitialised) {
                minCostUninitialised = false;
                minCost = nc.getCost();
            }
        }

        if (minCostUninitialised) {
            System.out.println("optor> didn't find any replica for LFN " + lfns);
//                continue;
        }

        aggregatedCost += minCost;
        //       }

        return aggregatedCost;
    }

}
