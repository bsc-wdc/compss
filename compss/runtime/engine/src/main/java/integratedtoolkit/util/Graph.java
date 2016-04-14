package integratedtoolkit.util;

import integratedtoolkit.types.Task;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;


/**
 * The Graph class represents a mathematical graph-theory oriented graph. It
 * contains a set of vertices with their own id and a set of edges that connect
 * a pair of vertices.
 *
 * @param <>> Object type of the vertices Ids
 * @param <>> Object type represented on the vertices
 */
public class Graph<K, T> {

    /**
     * relation vertice id --> vertice + edges
     */
    private Map<K, Node<T>> graph;

    /**
     * Constructs a new graph
     */
    public Graph() {
        graph = new TreeMap<K, Node<T>>();
    }

    /**
     * Returns the vertex associated with that id
     *
     * @param key id of the vertex
     * @return object on that vertex
     */
    public T get(K key) {
        Node<T> n = graph.get(key);
        if (n == null) {
            return null;
        }
        return n.getElement();
    }

    /**
     * Looks for all the vertices who are predecessors of the vetex with key
     * identifier.
     *
     * @param key identifier of the vertex
     * @return a set of elements with some edges pointing to key vertex
     * @throws ElementNotFoundException There is no vertex with key identifier
     */
    public Set<T> getPredecessors(K key) throws ElementNotFoundException {
        Node<T> n = graph.get(key);
        if (n == null) {
            throw new ElementNotFoundException();
        }
        return n.getPredecessors();
    }

    /**
     * Looks for all the vertices who are successors of the vetex with key as
     * identifier.
     *
     * @param key identifier of the vertex
     * @return a set of elements with at least an edge which points them from
     * the vertex with identifier key
     * @throws ElementNotFoundException There is no vertex with identifier key
     */
    public Set<T> getSuccessors(K key) throws ElementNotFoundException {
        Node<T> n = graph.get(key);
        if (n == null) {
            throw new ElementNotFoundException();
        }
        return n.getSuccessors();
    }

    /**
     * Gets an iterator over all the vertices who are predecessors of the one
     * whose identifier is key.
     *
     * @param key Identifier of the vertex
     * @return iterator over all the vertex with at least 1 edge pointing to the
     * node with identifier key
     * @throws ElementNotFoundException There is no vertex with identifier key
     */
    public Iterator<T> getIteratorOverPredecessors(K key) throws ElementNotFoundException {
        Node<T> n = graph.get(key);
        if (n == null) {
            throw new ElementNotFoundException();
        }
        return n.getIteratorOverPredecessors();
    }

    /**
     * Gets an iterator over all the vertices who are successors of the one
     * whose identifier is key.
     *
     * @param key Identifier of the vertex
     * @return iterator over all the vertex with at least 1 edge which points to
     * it with source the vertex with identifier key
     * @throws ElementNotFoundException There is no vertex with identifier key
     */
    public Iterator<T> getIteratorOverSuccessors(K key) throws ElementNotFoundException {
        Node<T> n = graph.get(key);
        if (n == null) {
            throw new ElementNotFoundException();
        }
        return n.getIteratorOverSuccessors();
    }

    /**
     * Returns the amount of vertices in the graph
     *
     * @return The amount of vertices in the graph
     */
    public int getSize() {
        return graph.size();
    }

    /**
     * Looks if the vertex with identifier key has any edge pointing to it
     *
     * @param key identifier of the vertex
     * @return true if the vertex with identifier key has any predecessor
     * @throws ElementNotFoundException There is no vertex with identifier key
     */
    public boolean hasPredecessors(K key) throws ElementNotFoundException {
        return !this.getPredecessors(key).isEmpty();
    }

    /**
     * Looks if the vertex with identifier key is the source of any edge
     *
     * @param key identifier of the vertex
     * @return true if the vertex with identifier key has any predecessor
     * @throws ElementNotFoundException There is no vertex with identifier key
     */
    public boolean hasSuccessors(K key) throws ElementNotFoundException {
        return !this.getSuccessors(key).isEmpty();
    }

    /**
     * Adds a new vertex to the graph without edges
     *
     * @param key identifier of the new vertex
     * @param element object represented by this vertex
     */
    public void addNode(K key, T element) {
        graph.put(key, new Node<T>(element));
    }

    /**
     * Adds a new edge between two already existing vertices
     *
     * @param sourceKey identifier of the source vertex of the edge
     * @param destKey identifier of the destination vertex of the edge
     * @throws ElementNotFoundException Any of the vertices does not exist
     */
    public void addEdge(K sourceKey, K destKey) throws ElementNotFoundException {
        Node<T> pred = graph.get(sourceKey);
        Node<T> succ = graph.get(destKey);

        if (pred == null || succ == null) {
            throw new ElementNotFoundException("Cannot add the edge: predecessor and/or successor don't exist");
        }

        pred.addSuccessor(succ.getElement());
        succ.addPredecessor(pred.getElement());
    }

    /**
     * Removes the vertex of the graph with identifier key
     *
     * @param key identifier of the vertex to be removed
     * @return the removed vertex.
     */
    public T removeNode(K key) {
        Node<T> n = graph.remove(key);
        if (n != null) {
            return n.getElement();
        }
        return null;
    }

    /**
     * Removes an edge of the graph
     *
     * @param sourceKey source vertex identifier
     * @param destKey destination vertex identifier
     * @throws ElementNotFoundException Any of the vertices does not exist
     */
    public void removeEdge(K sourceKey, K destKey) throws ElementNotFoundException {
        Node<T> pred = graph.get(sourceKey);
        Node<T> succ = graph.get(destKey);

        if (pred == null || succ == null) {
            throw new ElementNotFoundException("Cannot remove the edge: predecessor and/or successor don't exist");
        }

        pred.removeSuccessor(succ.getElement());
        succ.removePredecessor(pred.getElement());
    }

    /**
     * Removes all the vertices and edges in the graph
     */
    public void clear() {
        graph.clear();
    }

    /**
     * Creates a string with the description of the current state of the tasks
     * in the graph and its dependencies following the dot format
     *
     * @return graph state in dot format
     */
    public String getGraphDotFormat() {
        StringBuilder nodes = new StringBuilder();
        StringBuilder edges = new StringBuilder();
        for (Entry<K, Node<T>> e : graph.entrySet()) {
            Task t = (Task) (e.getValue().getElement());
            nodes.append(t.getDotDescription()).append("\n");
            for (T task : e.getValue().getSuccessors()) {
                edges.append(e.getKey()).append(" -> ").append(((Task) task).getId()).append("\n");
            }
        }
        return nodes.toString() + edges.toString();
    }

    /**
     * Class that represents a vertex and all its relations
     *
     * @param <>> Represented object type
     */
    private class Node<E> {

        // Node fields
        /**
         * represented object
         */
        private E element;
        /**
         * set of object with one edge pointing to it
         */
        private TreeSet<E> predecessors;
        /**
         * set of objects with
         */
        private TreeSet<E> successors;

        /**
         * Constructs a new Node
         *
         * @param element object represented by the vertex
         */
        public Node(E element) {
            this.element = element;
            this.predecessors = new TreeSet<E>();
            this.successors = new TreeSet<E>();
        }

        /**
         * Returns the represented object
         *
         * @return the represented object
         */
        public E getElement() {
            return element;
        }

        /**
         * Gets all the objects who are predecessors of this one
         *
         * @return set of object with at least 1 edge pointing to this node
         */
        public Set<E> getPredecessors() {
            return predecessors;
        }

        /**
         * Gets all the objects who are successors of this one
         *
         * @return set of object with at least 1 edge starting on this node
         */
        public Set<E> getSuccessors() {
            return successors;
        }

        /**
         * Gets an iterator over all the objects that are predecessors of this
         * one
         *
         * @return Iterator over all the objects with at least 1 edge pointing
         * to this node
         */
        @SuppressWarnings("unchecked")
        public Iterator<E> getIteratorOverPredecessors() {
            return ((Set<E>) predecessors.clone()).iterator();
        }

        /**
         * Gets an iterator over all the objects that are successors of this one
         *
         * @return Iterator over all the objects with at least 1 edge starting
         * on this node
         */
        @SuppressWarnings("unchecked")
        public Iterator<E> getIteratorOverSuccessors() {
            return ((Set<E>) successors.clone()).iterator();
        }

        /**
         * Adds a new Predecessor to the node
         *
         * @param pred object which precedes this one
         */
        public void addPredecessor(E pred) {
            predecessors.add(pred);
        }

        /**
         * Adds a new successor to the node
         *
         * @param pred object which successes this one
         */
        public void addSuccessor(E succ) {
            successors.add(succ);
        }

        /**
         * Removes a predecessor of the node
         *
         * @param pred object which won't precedes this one no more
         */
        public void removePredecessor(E pred) {
            predecessors.remove(pred);
        }

        /**
         * Removes a successor of the node
         *
         * @param pred object which won't success this one no more
         */
        public void removeSuccessor(E succ) {
            successors.remove(succ);
        }
    }
}
