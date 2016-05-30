package org.edg.data.replication.optorsim;

/**
 * This interface must be implemented by all Access Pattern
 * Generators. The access pattern defines the order in which 
 * the files are requested for each job. The getNextFile returns
 * the name of the next DataFile the CE needs to access.
 * <p>
 * Copyright (c) 2002 CERN, ITC-irst, PPARC, on behalf of the EU DataGrid.
 * For license conditions see LICENSE file or
 * <a href="http://www.edg.org/license.html">http://www.edg.org/license.html</a>
 * <p>
 * @since JDK1.4
 */
public interface AccessPatternGenerator {

    /**
     * Returns the name of the next DataFile to be processed by
     * the ComputingElement, according to the implementing
     * access pattern generator.
     */
    public String getNextFile();
}
