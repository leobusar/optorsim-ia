package org.edg.data.replication.optorsim.infrastructure;

import java.util.Stack;
import java.util.Vector;
import java.util.Enumeration;

/**
 * The FileTransferFactory handles all the {@link FileTransfer}s.
 * It has a Stack of FileTransfer objects which are currently not
 * in use and a Vector of FileTransfer objects which are currently
 * in use transferring files. The GridContainer calls getFileTransfer()
 * to get an unused FileTransfer, either one from the Stack or a
 * new instance is created if the Stack is empty. The GridContainer
 * then calls the transferFile() method of FileTransfer to copy
 * the file.
 * <p>
 * When each file transfer starts or ends, it notifies every other
 * file transfer using notifyAllFileTransferStarted() or
 * notifyAllFileTransferEnded() so they can
 * recalculate their estimated transfer times.
 * <p>
 * Copyright (c) 2002 CERN, ITC-irst, PPARC, on behalf of the EU DataGrid.
 * For license conditions see LICENSE file or
 * <a href="http://www.edg.org/license.html">http://www.edg.org/license.html</a>
 * <p>
 * @since JDK1.4
 */
abstract public class FileTransferFactory {

    // the FileTransfers currently not in use
    private static Stack _spareFileTransfers = new Stack();

    // the FileTransfers currently transferring files
    private static Vector _runningFileTransfers = new Vector();

    /**
     * Called by the {@link GridContainer} when it wants a FileTransfer
     * to copy or replicate a file. It returns a FileTransfer object
     * (either from the Stack of spare ones or by creating a new one)
     * so the GridContainer can call its transferFile() method.
     *
     * @return A FileTransfer object the GridContainer can use to
     * transfer a file.
     */
    protected static synchronized FileTransfer getFileTransfer() {

	FileTransfer fileTransfer;

	if(_spareFileTransfers.empty()) fileTransfer = new FileTransfer();
	else fileTransfer = (FileTransfer)_spareFileTransfers.pop();

	_runningFileTransfers.add(fileTransfer);

	return fileTransfer;
    }

    /**
     * Notifies all currently running FileTransfers that a
     * FileTransfer has started, so they can adjust 
     * their estimated file transfer time based on the new network
     * situation.
     * @param ft The FileTransfer calling this method.
     */
    protected static synchronized void notifyAllFileTransferStarted(FileTransfer ft) {

	notifyAllFileTransfers(ft);
    }

    /**
     * Notifies all currently running FileTransfers that a
     * FileTransfer has ended, so they can adjust 
     * their estimated file transfer time based on the new network
     * situation. The FileTransfer ft is taken off the running
     * file transfers Vector and put on the spare file transfers Stack.
     * @param ft The FileTransfer calling this method.
     */
    protected static synchronized void notifyAllFileTransferEnded(FileTransfer ft) {

	notifyAllFileTransfers(ft);
	_runningFileTransfers.remove(ft);
	_spareFileTransfers.push(ft);
    }

    /**
     * Wake up all the current FileTransfers.
     */
    private static void notifyAllFileTransfers(FileTransfer ft) {

	for(Enumeration e = _runningFileTransfers.elements();
	    e.hasMoreElements();) {
	    
	    FileTransfer f = (FileTransfer)e.nextElement();

	    if(!f.equals(ft)) {
            f.notifyFT();
        }
	}
    }
}
