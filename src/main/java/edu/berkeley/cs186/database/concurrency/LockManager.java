package edu.berkeley.cs186.database.concurrency;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The LockManager provides allows for table-level locking by keeping
 * track of which transactions own and are waiting for locks on specific tables.
 * <p>
 * THIS CODE IS FOR PROJECT 3.
 */
public class LockManager {

    public enum LockType {SHARED, EXCLUSIVE}

    ;
    private ConcurrentHashMap<String, Lock> tableNameToLock;
    private WaitsForGraph waitsForGraph;

    public LockManager() {
        tableNameToLock = new ConcurrentHashMap<String, Lock>();
        waitsForGraph = new WaitsForGraph();
    }

    /**
     * Acquires a lock on tableName of type lockType for transaction transNum.
     *
     * @param tableName the database to lock on
     * @param transNum  the transactions id
     * @param lockType  the type of lock
     */
    public void acquireLock(String tableName, long transNum, LockType lockType) {
        if (!this.tableNameToLock.containsKey(tableName)) {
            this.tableNameToLock.put(tableName, new Lock(lockType));
        }
        Lock lock = this.tableNameToLock.get(tableName);

        handlePotentialDeadlock(lock, transNum, lockType);

        lock.acquire(transNum, lockType);

    }

    /**
     * Adds any nodes/edges caused the by the specified LockRequest to
     * this LockManager's WaitsForGraph
     *
     * @param lock     the lock on the table that the LockRequest is for
     * @param transNum the transNum of the lock request
     * @param lockType the lockType of the lock request
     */
    private void handlePotentialDeadlock(Lock lock, long transNum, LockType lockType) {
        //This case is when the lock is SHARED and the current request wants an EXCLUSIVE lock
        //This means the current request will have to wait for all the SHARED lock owners
        if (lock.getType().equals(LockType.SHARED) && lockType.equals(LockType.EXCLUSIVE)) {
            //Iterator through all the SHARED lock owners
            java.util.Iterator<Long> iter = lock.getOwners().iterator();
            //In this loop, we test if the current request will cause a Deadlock with all the SHARED lock owvers
            while (iter.hasNext()) {
                long waitsForTransNum = iter.next();
                if (this.waitsForGraph.edgeCausesCycle(transNum, waitsForTransNum)) {
                    throw new DeadlockException("DeadlockException");
                }
            }
            //Escaping this loop means no Deadlock
            //So we repeat the loop and go ahead and add all the edges into the Waits-For graph
            iter = lock.getOwners().iterator();
            while (iter.hasNext()) {
                long waitsForTransNum = iter.next();
                this.waitsForGraph.addEdge(transNum, waitsForTransNum);
            }
        } else if (lock.getType().equals(LockType.EXCLUSIVE)) { //This case is when the lock is EXCLUSIVE
            //This means that the current request will have to wait for the EXCLUSIVE lock owner no matter what
            //This is to guarantee that we don't have an empty owner set
            if (lock.getSize() != 0) {
                //This iterator will only have 1 element since the lock is EXCLUSIVE
                java.util.Iterator<Long> iter = lock.getOwners().iterator();
                long waitsForTransNum = iter.next();
                //Check to see if the current request will cause a Deadlock
                if (this.waitsForGraph.edgeCausesCycle(transNum, waitsForTransNum)) {
                    throw new DeadlockException("DeadlockException");
                } else { //Entering here means we're safe and we can add the edge into the Waits-For graph
                    this.waitsForGraph.addEdge(transNum, waitsForTransNum);
                }
            }
        }
    }


    /**
     * Releases transNum's lock on tableName.
     *
     * @param tableName the table that was locked
     * @param transNum  the transaction that held the lock
     */
    public void releaseLock(String tableName, long transNum) {
        if (this.tableNameToLock.containsKey(tableName)) {
            Lock lock = this.tableNameToLock.get(tableName);
            lock.release(transNum);
        }
    }

    /**
     * Returns a boolean indicating whether or not transNum holds a lock of type lt on tableName.
     *
     * @param tableName the table that we're checking
     * @param transNum  the transaction that we're checking for
     * @param lockType  the lock type
     * @return whether the lock is held or not
     */
    public boolean holdsLock(String tableName, long transNum, LockType lockType) {
        if (!this.tableNameToLock.containsKey(tableName)) {
            return false;
        }

        Lock lock = this.tableNameToLock.get(tableName);
        return lock.holds(transNum, lockType);
    }
}
