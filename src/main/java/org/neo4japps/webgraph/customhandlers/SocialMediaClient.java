package org.neo4japps.webgraph.customhandlers;

public interface SocialMediaClient {

    public abstract int getSocialCount(String pageUrl) throws Exception;

    public abstract void setPolitenessDelay(int milliseconds);
}