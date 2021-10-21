package org.neo4japps.webgraph.importer;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public abstract class ConcurrentObservable {

    private final List<GraphObserver> observers = new CopyOnWriteArrayList<>();

    public final List<GraphObserver> getObservers() {
        return Collections.unmodifiableList(observers);
    }

    public final int countObservers() {
        return observers.size();
    }

    public final void addObserver(GraphObserver o) {
        observers.add(o);
    }

    public final void notifyObservers(PageNodesModificationEvent event) {
        for (GraphObserver observer : observers) {
            observer.update(this, event);
        }
    }
}
