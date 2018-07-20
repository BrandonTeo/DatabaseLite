package edu.berkeley.cs186.database.concurrency;

import java.util.*;

/**
 * A waits for graph for the lock manager (used to detect if
 * deadlock will occur and throw a DeadlockException if it does).
 */
public class WaitsForGraph {

    // We store the directed graph as an adjacency list where each node (transaction) is
    // mapped to a list of the nodes it has an edge to.
    private Map<Long, ArrayList<Long>> graph;
    private HashMap<Long, Boolean> marked;
    private HashMap<Long, Boolean> onStack;

    public WaitsForGraph() {
        graph = new HashMap<Long, ArrayList<Long>>();
    }

    public boolean containsNode(long transNum) {
        return graph.containsKey(transNum);
    }

    protected void addNode(long transNum) {
        if (!graph.containsKey(transNum)) {
            graph.put(transNum, new ArrayList<Long>());
        }
    }

    protected void addEdge(long from, long to) {
        if (!this.edgeExists(from, to)) {
            ArrayList<Long> edges = graph.get(from);
            edges.add(to);
        }
    }

    protected void removeEdge(long from, long to) {
        if (this.edgeExists(from, to)) {
            ArrayList<Long> edges = graph.get(from);
            edges.remove(to);
        }
    }

    protected boolean edgeExists(long from, long to) {
        if (!graph.containsKey(from)) {
            return false;
        }
        ArrayList<Long> edges = graph.get(from);
        return edges.contains(to);
    }

    /**
     * Checks if adding the edge specified by to and from would cause a cycle in this
     * WaitsForGraph. Does not actually modify the graph in any way.
     *
     * @param from the transNum from which the edge points
     * @param to   the transNum to which the edge points
     * @return
     */
    protected boolean edgeCausesCycle(long from, long to) {
        //No matter what we add the transNum(s) as nodes into the graph
        //We only choose when it comes to deciding whether to add edges
        this.addNode(from);
        this.addNode(to);

        //Self loops do not cause a cycle in this context
        if (from == to) {
            return false;
        }

        //Add the edge in question
        this.addEdge(from, to);

        //Test for cycles and store result
        //Main idea of this cycle test is taken from http://www.geeksforgeeks.org/detect-cycle-in-a-graph/
        boolean result = testForCycle();

        //Remove the edge in question
        this.removeEdge(from, to);

        //Return result
        return result;
    }

    //Implementation based off pseudocode from http://www.geeksforgeeks.org/detect-cycle-in-a-graph/
    private boolean testForCycle() {
        this.marked = new HashMap<>();
        this.onStack = new HashMap<>();

        java.util.Iterator<Long> transNumIter = this.graph.keySet().iterator();
        while (transNumIter.hasNext()) {
            long transNum = transNumIter.next();
            this.marked.put(transNum, false);
            this.onStack.put(transNum, false);
        }

        transNumIter = this.graph.keySet().iterator();
        while (transNumIter.hasNext()) {
            long transNum = transNumIter.next();
            if (testForCycleHelper(transNum)) {
                return true;
            }
        }
        return false;

    }

    //Implementation based off pseudocode from http://www.geeksforgeeks.org/detect-cycle-in-a-graph/
    private boolean testForCycleHelper(long transNum) {
        if (!this.marked.get(transNum)) {
            this.marked.put(transNum, true);
            this.onStack.put(transNum, true);

            java.util.Iterator<Long> adjIter = this.graph.get(transNum).iterator();
            while (adjIter.hasNext()) {
                long num = adjIter.next();
                if (!this.marked.get(num) && testForCycleHelper(num)) {
                    return true;
                } else if (this.onStack.get(num)) {
                    return true;
                }
            }
        }
        this.onStack.put(transNum, false);
        return false;
    }

}
