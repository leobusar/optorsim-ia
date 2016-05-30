package org.edg.data.replication.optorsim.infrastructure;

/**
 * SimpleBandwidth represents a network link whose capacity never
 * changes and which only carries Grid traffic.
 * <p>
 * Copyright (c) 2004 CERN, ITC-irst, PPARC, on behalf of the EU DataGrid.
 * For license conditions see LICENSE file or
 * <a href="http://www.edg.org/license.html">http://www.edg.org/license.html</a>
 * <p>
 * @since JDK1.4
 */

class SimpleBandwidth implements Bandwidth {
	private float _maxBandwidth;
	private int _nConnections=0;
		
	SimpleBandwidth( float bandwidth) {
		_maxBandwidth = bandwidth;
	}
				
	public void addConnection() {
		_nConnections++;
	}
		
	public void dropConnection() {
		_nConnections--;
	}

	public float availableBandwidth() {
		return _maxBandwidth / (_nConnections+1);
	}
	
	public float currentBandwidth() {
	    // in theory we don't need the == 0 test since this is only
	    // called for links with file transfers currently happening
		return (_nConnections == 0) ? _maxBandwidth : _maxBandwidth / _nConnections;
	}

	public float maxBandwidth() {
		return _maxBandwidth;
	}
	
	public int connectionCount() {
		return _nConnections;
	}
} 
