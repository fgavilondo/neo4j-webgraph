package org.neo4japps.webgraph.customhandlers;

import org.neo4j.graphdb.Node;
import org.neo4japps.webgraph.importer.ApplicationConfiguration;
import org.neo4japps.webgraph.importer.GraphImporter;
import org.neo4japps.webgraph.importer.PageNode;

public class TwitterEventHandler extends SocialMediaEventHandler {
    private SocialMediaClient twitterClient;

    /**
     * For unit test mocking
     * 
     * @param twitterClient
     *            the twitterClient to set
     */
    void setTwitterClient(SocialMediaClient twitterClient) {
        this.twitterClient = twitterClient;
    }

    @Override
    public void configure(ApplicationConfiguration config) {
        super.configure(config);
        if (twitterClient == null) {
            twitterClient = new TwitterClient(httpClient);
        }
        // Twitter doesn't like being hit too often
        twitterClient.setPolitenessDelay(config.getPolitenessDelay() + 100);
    }

    @Override
    protected int getTransactionSize() {
        return 1;
    }

    @Override
    protected boolean shouldIgnore(Node page, GraphImporter graphImporter) {
        return PageNode.hasTwitterCountProperty(page, graphImporter.getLock());
    }

    @Override
    protected Node updatePage(Node page, GraphImporter graphImporter) throws Exception {
        final String pageUrl = PageNode.getUrl(page, graphImporter.getLock());

        int count = twitterClient.getSocialCount(pageUrl);
        if (count != SocialJsonResponse.UNINITALIZED) {
            PageNode.setTwitterCount(page, count, graphImporter.getLock());
        }

        return page;
    }
}
