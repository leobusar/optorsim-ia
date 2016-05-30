package org.edg.data.replication.optorsim;

import org.edg.data.replication.optorsim.infrastructure.OptorSimParameters;

/**
 * Used to get the required group of users specified in the parameters
 * file.
 * <p/>
 * Copyright (c) 2004 CERN, ITC-irst, PPARC, on behalf of the EU DataGrid.
 *  For license conditions see LICENSE file or <a
 * href="http://www.edg.org/license.html">http://www.edg.org/license.html</a>
 * <p/>
 * 
 * @since JDK1.4
 */
abstract public class UsersFactory {

    private static final int SIMPLE_USERS = 1;
    private static final int RANDOM_WAIT_USERS = 2;
    private static final int CMSDC04_USERS = 3;

    /**
     * Returns the required instance of Users.
     */
    public static Users getUsers() {
        OptorSimParameters params = OptorSimParameters.getInstance();

        switch (params.getUsers()) {
            case SIMPLE_USERS:
                return new SimpleUsers();
            case RANDOM_WAIT_USERS:
                return new RandomWaitUsers();
            case CMSDC04_USERS:
                return new CMSDC04Users();
            default:
                System.out.println("ERROR: unknown group of users: "+
                        params.getUsers());
                System.exit(1);
        }
        return null;
    }
}
