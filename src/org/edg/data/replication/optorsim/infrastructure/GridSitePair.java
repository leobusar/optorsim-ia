package org.edg.data.replication.optorsim.infrastructure;

/**
 * Class that implements an unordered pair of grid sites.
 * <p>
 * Copyright (c) 2004 CERN, ITC-irst, PPARC, on behalf of the EU DataGrid.
 * For license conditions see LICENSE file or
 * <a href="http://www.edg.org/license.html">http://www.edg.org/license.html</a>
 * <p>
 * @since JDK1.4
 * @author paulm
 */

class GridSitePair {
	private int _hashCode;
	private GridSite _siteA;
	private GridSite _siteB;
		
	/**
	 * Construct a new GridSitePair.
	 * @param a The first GridSite to add
	 * @param b The second GridSite to add.
	 */
	protected GridSitePair( GridSite a, GridSite b) {
		_siteA = a;
		_siteB = b;

		int hashA = a.hashCode() & ( (1<<16) -1);
		int hashB = b.hashCode() & ( (1<<16) -1);

		int and = hashA & hashB;
		int or   = hashA | hashB;

		_hashCode = and << 16 | or;
	}
	
	/**
	 * Return the hash code value for this GridSitePair object.
	 * For more information see {@link Object}.
	 */
	public int hashCode() {
		return _hashCode;
	}
		
	/**
	 * Test whether a given object is equal to this GridSitePair.
	 */
	public boolean equals( Object obj) {
		if( ! (obj instanceof GridSitePair))
			return false;
		GridSitePair gsp = (GridSitePair) obj;
		
		if( gsp._hashCode != this._hashCode)
			return false;
			
		if( ((gsp._siteA == this._siteA) && (gsp._siteB == this._siteB)) ||
			((gsp._siteA == this._siteB) && (gsp._siteB == this._siteA)))
			return true; 
		return false;
	}		
}
