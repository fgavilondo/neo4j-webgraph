package org.neo4japps.webgraph.customhandlers;

import org.neo4j.graphdb.Node;
import org.neo4japps.webgraph.importer.ApplicationConfiguration;
import org.neo4japps.webgraph.importer.GraphImporter;
import org.neo4japps.webgraph.importer.PageNode;

public class FacebookEventHandler extends SocialMediaEventHandler {
    private SocialMediaClient facebookClient;

    /**
     * For unit test mocking
     * 
     * @param facebookClient
     *            the facebookClient to set
     */
    void setFacebookClient(SocialMediaClient facebookClient) {
        this.facebookClient = facebookClient;
    }

    @Override
    public void configure(ApplicationConfiguration config) {
        super.configure(config);
        if (facebookClient == null) {
            facebookClient = new FacebookClient(httpClient);
        }
        facebookClient.setPolitenessDelay(config.getPolitenessDelay());
    }

    @Override
    protected int getTransactionSize() {
        return 1;
    }

    @Override
    protected boolean shouldIgnore(Node page, GraphImporter graphImporter) {
        return PageNode.hasFacebookTotalCountProperty(page, graphImporter.getLock());
    }

    @Override
    protected Node updatePage(Node page, GraphImporter graphImporter) throws Exception {
        final String pageUrl = PageNode.getUrl(page, graphImporter.getLock());

        int count = facebookClient.getSocialCount(pageUrl);
        if (count != SocialJsonResponse.UNINITALIZED) {
            PageNode.setFacebookTotalCount(page, count, graphImporter.getLock());
        }

        return page;
    }
}
