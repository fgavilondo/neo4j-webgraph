package org.neo4japps.webgraph.customhandlers;

import org.neo4j.graphdb.Node;
import org.neo4japps.webgraph.importer.ApplicationConfiguration;
import org.neo4japps.webgraph.importer.GraphImporter;
import org.neo4japps.webgraph.importer.PageNode;

public class FacebookAndTwitterEventHandler extends SocialMediaEventHandler {
    private SocialMediaClient facebookClient;
    private SocialMediaClient twitterClient;

    /**
     * For unit test mocking
     *
     * @param facebookClient the facebookClient to set
     */
    void setFacebookClient(SocialMediaClient facebookClient) {
        this.facebookClient = facebookClient;
    }

    /**
     * For unit test mocking
     *
     * @param twitterClient the twitterClient to set
     */
    void setTwitterClient(SocialMediaClient twitterClient) {
        this.twitterClient = twitterClient;
    }

    @Override
    public void configure(ApplicationConfiguration config) {
        super.configure(config);

        if (facebookClient == null) {
            facebookClient = new FacebookClient(httpClient);
        }
        facebookClient.setPolitenessDelay(config.getPolitenessDelay());

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
        graphImporter.getLock().lock();
        try {
            return PageNode.hasFacebookTotalCountProperty(page) && PageNode.hasTwitterCountProperty(page);
        } finally {
            graphImporter.getLock().unlock();
        }
    }

    @Override
    protected Node updatePage(Node page, GraphImporter graphImporter) throws Exception {
        updateFacebook(page, graphImporter);
        updateTwitter(page, graphImporter);
        return page;
    }

    private void updateFacebook(Node page, GraphImporter graphImporter) throws Exception {
        if (PageNode.hasFacebookTotalCountProperty(page, graphImporter.getLock())) {
            return;
        }

        final String pageUrl = PageNode.getUrl(page, graphImporter.getLock());
        int count = facebookClient.getSocialCount(pageUrl);
        if (count != SocialJsonResponse.UNINITALIZED) {
            PageNode.setFacebookTotalCount(page, count, graphImporter.getLock());
        }
    }

    private void updateTwitter(Node page, GraphImporter graphImporter) throws Exception {
        if (PageNode.hasTwitterCountProperty(page, graphImporter.getLock())) {
            return;
        }

        final String pageUrl = PageNode.getUrl(page, graphImporter.getLock());
        int count = twitterClient.getSocialCount(pageUrl);
        if (count != SocialJsonResponse.UNINITALIZED) {
            PageNode.setTwitterCount(page, count, graphImporter.getLock());
        }
    }
}
