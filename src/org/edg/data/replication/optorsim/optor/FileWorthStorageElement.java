package org.edg.data.replication.optorsim.optor;

/**
 * An interface which the economic models must implement as well as the
 * generic StorageElement interface.
 * <p>
 * Copyright (c) 2002 CERN, ITC-irst, PPARC, on behalf of the EU DataGrid.
 * For license conditions see LICENSE file or
 * <a href="http://www.edg.org/license.html">http://www.edg.org/license.html</a>
 * <p>
 * @author caitrian
 * @since JDK1.4
 */
public interface FileWorthStorageElement {

    /**
     * Returns the value of the file specified by <i>fileID</i>
     * according to the model used for valuing files.
     * @return The value of the file specified by <i>fileID</i>
     */
    double evaluateFileWorth(int fileID);

}
