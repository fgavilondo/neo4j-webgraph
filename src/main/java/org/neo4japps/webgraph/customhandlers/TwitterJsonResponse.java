package org.neo4japps.webgraph.customhandlers;

public class TwitterJsonResponse extends SocialJsonResponse {
    public int count = UNINITALIZED;

    @Override
    public String toString() {
        return "TwitterJsonResponse [count=" + count + "]";
    }

    public boolean isInitialized() {
        return count != UNINITALIZED;
    }

    @Override
    public int getCount() {
        return count;
    }
}
