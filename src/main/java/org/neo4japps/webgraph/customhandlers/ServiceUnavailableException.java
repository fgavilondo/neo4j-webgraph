package org.neo4japps.webgraph.customhandlers;

public class ServiceUnavailableException extends Exception {

    private static final long serialVersionUID = 1L;

    public ServiceUnavailableException(String message) {
        super(message);
    }
}
