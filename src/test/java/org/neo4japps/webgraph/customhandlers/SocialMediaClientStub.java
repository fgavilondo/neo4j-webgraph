package org.neo4japps.webgraph.customhandlers;


class SocialMediaClientStub implements SocialMediaClient {
    private final int count;

    public SocialMediaClientStub(int count) {
        this.count = count;
    }

    @Override
    public int getSocialCount(String pageUrl) throws Exception {
        return count;
    }

    @Override
    public void setPolitenessDelay(int milliseconds) {
        // no-op
    }
}
