package org.edg.data.replication.optorsim.infrastructure;

import org.edg.data.replication.optorsim.time.GridTime;
import org.edg.data.replication.optorsim.time.GridTimeFactory;

import java.util.*;

/**
 * Keeps all the output data from the simulation in the one
 * place, so it can all be printed out with one method call.
 * <p>
 * Copyright (c) 2002 CERN, ITC-irst, PPARC, on behalf of the EU DataGrid.
 * For license conditions see LICENSE file or
 * <a href="http://www.edg.org/license.html">http://www.edg.org/license.html</a>
 * <p>
 * @since JDK1.4
 */
public class Statistics {

	private Date _date;
	private Map _values;
	private Object _generator;
	private Set _children;  //child statistic of this one
    private GridTime _time = GridTimeFactory.getGridTime();

	/**
	 * Instanciate a new (set of) statistics
	 * @param gen  The object that generated this statistic
	 * @param values The mappings between attribute (String) and some value object
	 */
	public Statistics(Object gen, Map values) {
		_values = values;
		_generator = gen;
		_children = new HashSet();
		_date = _time.getDate();
	}

	/**
	 * Instanciate a new (set of) statistics
	 * @param gen  The object that generated this statistic
	 * @param values The mappings between attribute (String) and some value object
	 * @param children The statistics that are children to this measurement.
	 */
	public Statistics(Object gen, Map values, Set children) {
		_values = values;
		_generator = gen;
		_children = children;
		_date = _time.getDate();
	}

	
	/**
	 * Add a statistic to the Set of this statistics' children
	 * @param child The child statistic to add
	 */
	public void addChild( Statistics child) {
		_children.add( child);
	}
	

	/**
	 * Query the static for an attribute.  This is the arbitrary access point
	 * @param attribute  The String description of the Attribute
	 * @return the value of of that attribute, or null if there is none.
	 */
	public Object getStatistic( String attribute) {
		return _values.get(attribute);
	}
	
	/**
	 * Get an attribute that is known to be a Float.
	 * @param attribue The String describing the attribute
	 * @return the float value of that attribute
	 */
	public float getFloatStatistic( String attribue) {
		Float val;
		
		val = (Float) getStatistic( attribue);
		
		return val.floatValue();
	}

	
	/**
	 * Return the time that this object was generated
	 * @return when this statistic was taken
	 */
	public Date getDate() {
		return _date;		
	}
	
	/**
	 * Get the object that generated this statistic
	 * @return The generating object
	 */
	public Object getGenerator() {
		return _generator;
	}
	
	/**
	 * Overload the toString, so its meaningful
	 */
	public String toString() {
		return "Statistics for "+_generator;
	}
	
	/**
	 * Return a set of (recursively) all available statistics, effectively
	 * flattening the tree.
	 * @return A Set of all the statistics in this tree. 
	 */
	public Set allStatistics() {
		Set allStats = new HashSet();
		recursiveAllStats( allStats, null);
		return allStats;
	}
	
	/**
	 * Obtain all statistics from a given generator Class.  The statistics tree is
	 * parsed recursively to obtain this Set, so only match statistics within a
	 * subtree will be returned.
	 * @param classType The Class of generator you are interested in.
	 * @return The Set of all statistics generated from this Class.
	 */
	public Set allStatistics( Class classType) {
		Set allStats = new HashSet();
		recursiveAllStats( allStats, classType);
		return allStats;
	}
	
	/**
	 * Recursively traverse the tree, adding statistics to the Set
	 * @param stats The Set into which the statistics should be added.
	 * @param type The Class of generator to be considered, null if all
     * statistics should be included.
	 */
	private void recursiveAllStats( Set stats, Class type) {
		if( _children != null) {
			for( Iterator i=_children.iterator(); i.hasNext();) {
				Statistics stat = (Statistics) i.next();
				stat.recursiveAllStats( stats, type);
			}
		}

		if( (type == null) || (this._generator.getClass() == type)) 
			stats.add( this);
	}

	/**
	 * Method to dump the statistic onto stdout.
	 */
	public void printStatistics() {
		printStatistics( true, "", false, false);
	}
	

	/**
	 * Recursively display all statistics in this tree.
	 */
	public void recursivePrintStatistics() {
		recursivePrintStatistics( true, "", false);
	}
	
	/**
	 * Print all statistics, potentially with some additional decoration.
	 * @param topLevel boolean value indicating if this statistic should be flush-left, with no padding.
	 * @param pad a String of decoration that pads the current node's output.
	 * @param hasChildren should decoration be included for this node's children
	 * @param hasNext should decoration be included for this node's siblings that haven't been painted yet.
	 */
	private void printStatistics( boolean topLevel, String pad, boolean hasChildren, boolean hasNext) {
		String finalSpace, headerSpace, childSpace;

		if( ! topLevel ) {
			finalSpace= hasNext ? " | " : "   ";
			headerSpace=" +-";
			
			System.out.println( pad+" | ");
		} else {
			finalSpace="";
			headerSpace="";
		}
		childSpace= hasChildren ? " | " : "   ";
		
		System.out.println( pad+headerSpace+"Statistics for "+_generator+" taken "+_date);
		for( Iterator i=_values.keySet().iterator(); i.hasNext();) {
			Object key, value;
			key = i.next();
			if( !(key.equals("runnableStatus") || key.equals("queueLength") || key.equals("status") || key.equals("meanJobTime"))) {
			value = _values.get(key);
			System.out.println( pad+finalSpace+childSpace+key+" = "+value);
			}
		}		
	}

	/**
	 * Recursively display this statistic and all of the sub-tree, with
	 * increasing depth.
	 * @param topLevel boolean value indicating if this statistic is the topmost in the tree
	 * @param pad a String of what should pad the output (due to the recusion)
	 * @param hasNext boolean value indicating whether this node has sibblings that still need to be printed.
	 */
	private void recursivePrintStatistics( boolean topLevel, String pad, boolean hasNext) {
		String spacer = (! topLevel) ? (hasNext ? " | " : "   " ) : "";
		int nChildren = _children.size();
				
		printStatistics( topLevel, pad, nChildren > 0, hasNext);
				
			// If we have no children, nothing more to do.
		if(  nChildren == 0)
			return;
		
		String newPad = new String( pad+spacer);
		topLevel = false;
		
		for( Iterator i=_children.iterator(); i.hasNext();) {
			Statistics stat = (Statistics) i.next();
			stat.recursivePrintStatistics( topLevel, newPad, i.hasNext());
		}
	}
	
}
