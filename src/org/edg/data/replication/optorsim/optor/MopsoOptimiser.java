package org.edg.data.replication.optorsim.optor;

import org.edg.data.replication.optorsim.infrastructure.DataFile;
import org.edg.data.replication.optorsim.infrastructure.GridSite;
import org.edg.data.replication.optorsim.auctions.AccessMediator;

import java.util.List;
import java.util.Iterator;
import java.util.LinkedList;
import org.edg.data.replication.optorsim.infrastructure.StorageElement;
import org.edg.data.replication.optorsim.reptorsim.ReplicaManager;

/**
 *
 * <p>
 * <p>
 * @author leobusta
 * @since JDK1.6
 */
public class MopsoOptimiser extends SkelOptor {

    public MopsoOptimiser(GridSite site) {

        super(site);
    }

    /**
     *
     */
    public DataFile[] getBestFile(String[] lfns, float[] fileFraction) {

        /**
         * Escoger un líder utilizando las replicas en el SE y las replicas
         * solicitadas. Inicialmente escogo los mejores candidatos utilizando la
         * distancia y el ancho de banda disponible. Este proceso lo realiza
         * SkelOptor. Si hay espacio disponible los replicamos, de lo contrario
         * es necesario comparar los nuevos archivos con los existentes,
         * utilizando nuestra función multiobjetivo
         */
        DataFile files[] = super.getBestFile(lfns, fileFraction);

        StorageElement closeSE = _site.getCloseSE();
        ReplicaManager rm = ReplicaManager.getInstance();

        if (closeSE != null) {
            //Determinar si hay espacio disponible
            int spaceRequired = 0;
            for (int i = 0; i < files.length; i++) {

                StorageElement se = files[i].se();
                // skip over any file stored on the local site
                if (se.getGridSite() == _site) {
                    continue;
                }
                spaceRequired += files[i].size();
            }

            /**
             * Si hay espacio no es necesario realizar ningún cálculo *
             */
            boolean replicate = false;
            if (spaceRequired <= closeSE.getAvailableSpace()) {
                replicate = true;
            }

            for (int i = 0; i < files.length; i++) {
                StorageElement se = files[i].se();

                // skip over any file stored on the local site
                if (se.getGridSite() == _site) {
                    continue;
                }

                if (replicate){
                    DataFile replicatedFile;

                    // Attempt to replicate file to close SE without delete files.
                    replicatedFile = rm.replicateFile(files[i], closeSE);

                    // If replication worked, store it and move on to next file (for loop)
                    if (replicatedFile != null) {
                        files[i].releasePin();
                        files[i] = replicatedFile;
                    }
                }
            }
            Mopso mopso = new Mopso(files);

            boolean[] bestFiles = mopso.getBestFilesIndex();
            
            for (int i = 0; i < files.length; i++) {

                StorageElement se = files[i].se();
                if (se.getGridSite() == _site || bestFiles[i]==false) {
                    continue;
                }

                DataFile replicatedFile;

                // Loop trying to delete a file on closeSE to make space
                do {

                    // Attempt to replicate file to close SE without delete files.
                    replicatedFile = rm.replicateFile(files[i], closeSE);

                    // If replication worked, store it and move on to next file (for loop)
                    if (replicatedFile != null) {
                        files[i].releasePin();
                        files[i] = replicatedFile;
                        break;
                    }

                    // If replication didn't work, try finding expendable files.
                    List expendable = chooseFilesToDelete(files[i], closeSE);

                    // didn't find one, fall back to remoteIO
                    if (expendable == null) {
                        break;
                    }

                    for (Iterator it = expendable.iterator(); it.hasNext();) {
                        rm.deleteFile((DataFile) it.next());
                    }

                } while (replicatedFile == null);

            } // for
        }
        return files;

    }

    /**
     * Tests whether the potential replica is more valuable than the deleteable
     * files already on the SE.
     *
     * @param potentialFileWorth the value of the possible replica
     * @param deleteableFiles the list of deleteable files stored on the SE.
     * @return true if the file is more valuable than existing files, false if
     * not.
     */
    protected boolean worthReplicating(double potentialFileWorth,
            List deleteableFiles) {

        double deleteableFilesValue = 0;
        for (Iterator i = deleteableFiles.iterator(); i.hasNext();) {
            DataFile file = (DataFile) i.next();
            deleteableFilesValue += file.lastEstimatedValue();
        }
        //TODO > or >=? >= would encourage more replication
        return potentialFileWorth > deleteableFilesValue;
    }

    /**
     * Uses the algorithm contained in {@link MopsoStorageElement} to determine
     * whether replication should take place, and if so which file should be
     * deleted.
     */
    protected List chooseFilesToDelete(DataFile file, StorageElement se) {

        MopsoStorageElement thisSE;
        if (se instanceof MopsoStorageElement) {
            thisSE = (MopsoStorageElement) se;
        } else {
            return null;
        }

        double potentialFileWorth = thisSE.evaluateFileWorth(file.fileIndex());
        List deleteableFiles = thisSE.filesToDelete(file);

        if (worthReplicating(potentialFileWorth, deleteableFiles)) {
            return deleteableFiles;
        }

        // not worth replicating so return null and do remote i/o
        return null;
    }

}
