package org.edg.data.replication.optorsim.time;

import java.util.HashMap;
import java.util.Map;

/**
 * When using EventDrivenGridTime, WaitObjects are used to keep track of
 * waiting threads. They are initialised with a long representing the
 * simulation time at which the thread is due to be woken up and the
 * Object which is to be waited on. The TimeAdvancer sorts WaitObjects into
 * order of wake up time, hence they must implement Comparable. The defined
 * order of WaitObjects uses the long value, and if two have the same they
 * are ordered by hashcode. If they are still the same (very unlikely but
 * possible) the hashcodes of the calling objects are used.
 * This is so no two WaitObjects are equal by equals().
 * <p>
 * (Originally simple Longs were used as wait objects but this caused
 * problems when two had the same value.)
 * <p/>
 * Copyright (c) 2004 CERN, ITC-irst, PPARC, on behalf of the EU DataGrid.
 * For license conditions see LICENSE file or
 * <a href="http://www.edg.org/license.html">http://www.edg.org/license.html</a>
 * <p/>
 * 
 * @since JDK1.4
 */
class WaitObject implements Comparable{

    private long _value;
    private Object _callingObj;
    private static Map _waitObjects = new HashMap();
    private int _nbWaiting ;
    
    /**
     * Used to get a WaitObject with the given notify time and Object
     * to wait on. A Map of Objects to WaitObjects is used so they can
     * be reused should the same object need to be waited on.
     * @param o The Object to wait on.
     * @param wakeUpTime The time it should be woken up.
     * @return The WaitObject associated with o or a new WaitObject if
     * none exists.
     */
    static synchronized WaitObject getWaitObject(Object o, long wakeUpTime) {
        if (_waitObjects.containsKey(o)) {
            WaitObject wo = (WaitObject) _waitObjects.get(o);
            wo.setWakeUp(wakeUpTime);
            return wo;
        }
        WaitObject wo = new WaitObject(o, wakeUpTime);
        _waitObjects.put(o, wo);
        return wo;
    }

    /**
     * Called in {@link EventDrivenGridTime#gtNotify(Object)} to get
     * the WaitObject associated with o.
     * @param o The Object being waited on.
     * @return The associated WaitObject or null if none exists.
     */
    static synchronized WaitObject getWaitObject(Object o) {
        return (WaitObject) _waitObjects.get(o);
    }

    /**
     * Every time time.gtSleep() is called a new Object is created to
     * wait on. To conserve memory, after the sleep has finished this
     * Object is removed from the wait objects map.
     * @param o The Object which is no longer needed.
     */
    static synchronized void discard(Object o) {
        _waitObjects.remove(o);
    }

    /**
     * Creates a new WaitObject vith the time at which it should
     * be woken up. Private since WaitObjects should be obtained
     * calling the static getWaitObject().
     * @param callingObj The object to be waited on.
     * @param value The time the WaitObject should be notified.
     */
    private WaitObject(Object callingObj, long value) {
        _value = value;
        _callingObj = callingObj;
        _nbWaiting=0;
    }

    /**
     * Returns the Object calling wait or sleep that is to be
     * waited on.
     */
    Object getCallingObj() {
        return _callingObj;
    }

    /**
     * Returns the time the WaitObject should be notified.
     */
    long longValue() {
        return _value;
    }

    /**
     * Set a new wake up time for this WaitObject.
     * @param wakeUpTime Time to wake up.
     */
    private void setWakeUp(long wakeUpTime) {
        _value = wakeUpTime;
    }

    public synchronized void addWaiter(){
        _nbWaiting ++;
    }
    
    public synchronized void removeWaiter(){
        _nbWaiting --;
    }
    
    public synchronized int getWaiter(){
        return _nbWaiting ;
    }
    
    
    
    /**
     * Compares this WaitObject to o firstly by using the long value
     * representing the wake up time, then if these are equal using
     * the hashcodes of the two, and if these are still equal, using
     * the hashcodes of the two calling threads associated with the
     * WaitObjects.
     * @param o The WaitObject to compare this to.
     */
    public int compareTo(Object o) {
        return compare((WaitObject)this, (WaitObject)o);
    }

    private int compare(WaitObject o1, WaitObject o2) {
        // to catch values too big to be ints
        if(o1.longValue() == Long.MAX_VALUE && o2.longValue() != Long.MAX_VALUE)
            return 1;
        if(o1.longValue() != Long.MAX_VALUE && o2.longValue() == Long.MAX_VALUE)
            return -1;

        // first use long value
        int i = (int) (o1.longValue() - o2.longValue());
        if (i != 0) return i;
        // then try WaitObject hashcode.
        int h = o1.hashCode() - o2.hashCode();
        if (h != 0) return h;
        // unlikely to get here but finally try calling object hashcode.
        return o1.getCallingObj().hashCode() - o2.getCallingObj().hashCode();
    }

}