package org.neo4japps.webgraph.customhandlers;

public interface SocialMediaClient {

    int getSocialCount(String pageUrl) throws Exception;

    void setPolitenessDelay(int milliseconds);
}