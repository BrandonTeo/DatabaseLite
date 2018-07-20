package edu.berkeley.cs186.database.concurrency;

import sun.security.provider.SHA;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Each table will have a lock object associated with it in order
 * to implement table-level locking. The lock will keep track of its
 * transaction owners, type, and the waiting queue.
 */
public class Lock {


    private Set<Long> transactionOwners;
    private ConcurrentLinkedQueue<LockRequest> transactionQueue;
    private LockManager.LockType type;

    public Lock(LockManager.LockType type) {
        this.transactionOwners = new HashSet<Long>();
        this.transactionQueue = new ConcurrentLinkedQueue<LockRequest>();
        this.type = type;
    }

    protected Set<Long> getOwners() {
        return this.transactionOwners;
    }

    public LockManager.LockType getType() {
        return this.type;
    }

    private void setType(LockManager.LockType newType) {
        this.type = newType;
    }

    public int getSize() {
        return this.transactionOwners.size();
    }

    public boolean isEmpty() {
        return this.transactionOwners.isEmpty();
    }

    private boolean containsTransaction(long transNum) {
        return this.transactionOwners.contains(transNum);
    }

    private void addToQueue(long transNum, LockManager.LockType lockType) {
        LockRequest lockRequest = new LockRequest(transNum, lockType);
        this.transactionQueue.add(lockRequest);
    }

    private void removeFromQueue(long transNum, LockManager.LockType lockType) {
        LockRequest lockRequest = new LockRequest(transNum, lockType);
        this.transactionQueue.remove(lockRequest);
    }

    private void addOwner(long transNum) {
        this.transactionOwners.add(transNum);
    }

    private void removeOwner(long transNum) {
        this.transactionOwners.remove(transNum);
    }

    /**
     * Attempts to resolve the specified lockRequest. Adds the request to the queue
     * and calls wait() until the request can be promoted and removed from the queue.
     * It then modifies this lock's owners/type as necessary.
     *
     * @param transNum transNum of the lock request
     * @param lockType lockType of the lock request
     */
    protected synchronized void acquire(long transNum, LockManager.LockType lockType) {
        //If a transaction already has a EXCLUSIVE lock, we just block it by skipping it and not do anything
        if (!this.getType().equals(LockManager.LockType.EXCLUSIVE) || !(this.containsTransaction(transNum))) {
            //Add request to queue
            this.addToQueue(transNum, lockType);

            //Repeatedly check and wait until this transaction can obtain its designated lock
            while (!checkCompatibility(transNum, lockType)) {
                try {
                    this.wait();
                } catch (InterruptedException e) {}
            }

            //Reaching here means transaction obtained designated lock
            //So we remove request from the queue and add it to the owner set and update lockType
            this.removeFromQueue(transNum, lockType);
            this.addOwner(transNum);
            this.setType(lockType);
        }

    }

    //This method checks whether we are able to obtain the lock based on the current circumstances
    private boolean checkCompatibility(long transNum, LockManager.LockType lockType) {
        //This case is when the lock has no owner
        if (this.getSize() == 0) {
            //If the current request is the first one in the queue
            //We automatically grant it the lock it wants
            if (this.transactionQueue.peek().equals(new LockRequest(transNum, lockType))) {
                return true;
            } else { //This case is when the current request is not in front of the queue
                if (lockType.equals(LockManager.LockType.EXCLUSIVE)) { //Case when current transaction wants EXCLUSIVE lock
                    //Since the lock has no owner, there is no possibility of an upgrade
                    return false;
                } else { //Case when current request wants SHARED lock
                    //Iterator through all the requests
                    java.util.Iterator<LockRequest> iter = this.transactionQueue.iterator();
                    while (iter.hasNext()) { //Scan through all the requests in queue until we reach the current request
                        LockRequest lr = iter.next();
                        if (lr.equals(new LockRequest(transNum, lockType))) {
                            //Entering here means every request before the current request wants a SHARED lock too
                            //This means the current request can get a SHARED lock too
                            return true;
                        } else {
                            //If we reach an EXCLUSIVE lock request before the current request in the queue
                            //It means that we need to wait until it is resolved so we can't do anything but wait
                            if (lr.getLockType().equals(LockManager.LockType.EXCLUSIVE)) {
                                return false;
                            }
                            //Re-loop if scans through a SHARED lock request
                        }
                    }
                }
            }
        //The following cases are when the lock is being held by some owner(s)
        } else if (this.getType().equals(LockManager.LockType.EXCLUSIVE)) { //Case when lock is EXCLUSIVE and held by 1 transaction
            //We can't do anything but wait, so return false
            return false;
        } else { //Case when lock is SHARED and held by 1 or more transactions
            if (lockType.equals(LockManager.LockType.EXCLUSIVE)) { //Case when current transaction wants EXCLUSIVE lock
                //The only way we can get EXCLUSIVE lock is that we upgrade our lock from SHARED to EXCLUSIVE
                //This means there should only be 1 current owner and should be the same as the pending transaction
                return (this.getSize() == 1 && this.containsTransaction(transNum));
            } else { //Case when current request wants SHARED lock too
                //Iterator through all the requests
                java.util.Iterator<LockRequest> iter = this.transactionQueue.iterator();
                while (iter.hasNext()) { //Scan through all the requests in queue until we reach the current request
                    LockRequest lr = iter.next();
                    if (lr.equals(new LockRequest(transNum, lockType))) {
                        //Entering here means every request before the current request wants a SHARED lock too
                        //This means the current request can get a SHARED lock too
                        return true;
                    } else {
                        //If we reach an EXCLUSIVE lock request before the current request in the queue
                        //It means that we need to wait until it is resolved so we can't do anything but wait
                        if (lr.getLockType().equals(LockManager.LockType.EXCLUSIVE)) {
                            return false;
                        }
                        //Re-loop if scans through a SHARED lock request
                    }
                }
            }
        }
        return false;
    }


    /**
     * transNum releases ownership of this lock
     *
     * @param transNum transNum of transaction that is releasing ownership of this lock
     */
    protected synchronized void release(long transNum) {
        //We release the lock that transNum by removing it from the owner set
        this.removeOwner(transNum);
        //Need to notify all the waiting threads
        notifyAll();
    }

    /**
     * Checks if the specified transNum holds a lock of lockType on this lock object
     *
     * @param transNum transNum of lock request
     * @param lockType lock type of lock request
     * @return true if transNum holds the lock of type lockType
     */
    protected synchronized boolean holds(long transNum, LockManager.LockType lockType) {
        if (this.containsTransaction(transNum)) {
            //transNum is in the owner set therefore we compare the lockTypes
            return this.getType().equals(lockType);
        } else { //Enters when the transNum is not even in the owner set therefore no locks for it
            return false;
        }
    }

    /**
     * LockRequest objects keeps track of the transNum and lockType.
     * Two LockRequests are equal if they have the same transNum and lockType.
     */
    private class LockRequest {
        private long transNum;
        private LockManager.LockType lockType;

        private LockRequest(long transNum, LockManager.LockType lockType) {
            this.transNum = transNum;
            this.lockType = lockType;
        }

        @Override
        public int hashCode() {
            return (int) transNum;
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof LockRequest))
                return false;
            if (obj == this)
                return true;

            LockRequest rhs = (LockRequest) obj;
            return (this.transNum == rhs.transNum) && (this.lockType == rhs.lockType);
        }

        //Method to get the lockType of a LockRequest
        public LockManager.LockType getLockType() {
            return this.lockType;
        }

    }

}
