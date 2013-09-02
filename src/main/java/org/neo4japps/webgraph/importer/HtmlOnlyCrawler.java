package org.neo4japps.webgraph.importer;

import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import org.apache.http.HttpStatus;
import org.neo4j.graphdb.Node;
import org.neo4japps.webgraph.util.UrlUtil;

import edu.uci.ics.crawler4j.crawler.Page;
import edu.uci.ics.crawler4j.crawler.WebCrawler;
import edu.uci.ics.crawler4j.parser.HtmlParseData;
import edu.uci.ics.crawler4j.url.WebURL;

/**
 * WebCrawler class in the Runnable class that is executed by each crawler thread.
 */
public class HtmlOnlyCrawler extends WebCrawler {
    private static final String MEDIA_EXTENSION_REGEX = ".*(\\.(css|js|bmp|gif|jpe?g|png|ico|tiff?|mid|mp2|mp3|mp4|wav|avi|mov|mpeg|ram|m4v|pdf|rm|smil|wmv|swf|wma|zip|rar|gz))";
    private static final Pattern MEDIA_FILE_PATTERN = Pattern.compile(MEDIA_EXTENSION_REGEX + "$");
    private static final Pattern INCORRECT_MEDIA_PATTERN = Pattern.compile(MEDIA_EXTENSION_REGEX + "/$");

    private static final AtomicInteger atomicPageCounter = new AtomicInteger();

    private ApplicationConfiguration appConfig;
    private GraphImporter graphImporter;

    /**
     * For unit tests. Returns the number of pages visited by all instances of this class.
     * 
     * @return number of pages.
     */
    static int getGlobalPageCount() {
        return atomicPageCounter.get();
    }

    /**
     * For unit tests. Resets the number of pages to zero.
     */
    static void resetGlobalPageCount() {
        atomicPageCounter.set(0);
    }

    /**
     * For unit tests. Allows us to inject the configuration.
     * 
     * @param config
     *            the config
     */
    void setAppConfig(ApplicationConfiguration config) {
        this.appConfig = config;
    }

    /**
     * For unit tests. Allows us to inject the importer.
     * 
     * @param graphImporter
     *            the importer
     */
    void setGraphImporter(GraphImporter graphImporter) {
        this.graphImporter = graphImporter;
    }

    @Override
    public void onStart() {
        Object[] customData = (Object[]) getMyController().getCustomData();
        appConfig = (ApplicationConfiguration) customData[0];
        graphImporter = (GraphImporter) customData[1];
    }

    @Override
    public boolean shouldVisit(WebURL url) {
        return shouldVisit(url, null);
    }

    private boolean shouldVisit(WebURL url, Node parentNode) {
        final String lowerCaseUrl = url.getURL().toLowerCase();

        if (isMediaUrl(lowerCaseUrl)) {
            logger.trace("Ignoring " + url + ": media files, CSS and JavaScript URLs will not be crawled");
            return false;
        }

        if (INCORRECT_MEDIA_PATTERN.matcher(lowerCaseUrl).matches()) {
            String linkedFromStr = (parentNode == null) ? "" : " linked from " + PageNode.getUrl(parentNode);
            logger.warn("Ignoring incorrect media URL " + url + linkedFromStr);
            return false;
        }

        if (!appConfig.isCrawlableUrl(lowerCaseUrl)) {
            logger.trace("Ignoring " + url + ": this domain is not configured for crawling");
            try {
                final String host = UrlUtil.extractHost(lowerCaseUrl);
                // ideally we want to save the domain only, we do not care about each individual url
                if (host != null && !host.isEmpty()) {
                    addExcludedDomainToFailedUrls(host);
                } else {
                    addExcludedDomainToFailedUrls(url.getURL());
                }
            } catch (MalformedURLException e) {
                addExcludedDomainToFailedUrls(url.getURL());
            }
            return false;
        }

        return true;
    }

    private void addExcludedDomainToFailedUrls(String url) {
        FailedUrls.getInstance().add("ExcludedDomain", url, "This domain is not configured for crawling");
    }

    private boolean isMediaUrl(final String url) {
        return MEDIA_FILE_PATTERN.matcher(url).matches() || url.contains("/img/") || url.contains("/css/");
    }

    /**
     * This function is called once the header of a page is fetched. We log and record all URLs with a HTTP status code
     * of 307 or >= 400.
     */
    @Override
    protected void handlePageStatusCode(WebURL webUrl, int statusCode, String statusDescription) {
        String message = combine(statusCode, statusDescription);
        if (statusCode >= HttpStatus.SC_BAD_REQUEST) {
            FailedUrls.getInstance().add("FailedRequest", webUrl, message);
        }
        if (statusCode == HttpStatus.SC_TEMPORARY_REDIRECT) {
            FailedUrls.getInstance().add("TemporaryRedirect", webUrl, message);
        }
    }

    private String combine(int statusCode, String statusDescription) {
        return "(" + statusCode + ") " + statusDescription;
    }

    /**
     * This function is called when a page is fetched and ready to be processed. It is important that we don't let any
     * Graph exceptions escape this method as this would cause the calling thread to die and eventually the crawler
     * would run out of threads.
     */
    @Override
    public void visit(Page page) {
        WebURL webUrl = page.getWebURL();

        String statusMessage = FailedUrls.getInstance().getStatusMessage("FailedRequest", webUrl);
        if (statusMessage != null) {
            logger.warn("Ignoring bad URL " + webUrl + " - " + statusMessage);
            return;
        }

        int pageCounter = atomicPageCounter.incrementAndGet();

        if (graphImporter != null) {
            logger.info("Importing page # " + pageCounter + ": " + webUrl + " (node count so far: "
                    + graphImporter.getNumberOfPageNodes() + ")");

            if (page.getParseData() instanceof HtmlParseData) {
                visitHtmlPage(webUrl.getURL(), (HtmlParseData) page.getParseData());
            } else {
                visitNonHtmlPage(webUrl.getURL());
            }
        }
    }

    private void visitHtmlPage(String url, HtmlParseData htmlParseData) {
        try {
            Node pageNode = graphImporter.addPage(url, htmlParseData.getHtml());
            visitHtmlLinks(pageNode, htmlParseData.getOutgoingUrls());
        } catch (Exception e) {
            logger.error("Error creating node for " + url, e);
        }
    }

    private void visitHtmlLinks(Node pageNode, List<WebURL> links) {
        logger.trace("Number of outgoing links from " + PageNode.getUrl(pageNode) + ": " + links.size());

        if (links == null || links.isEmpty()) {
            return;
        }

        List<String> crawlableLinks = new ArrayList<String>(links.size());
        for (WebURL link : links) {
            if (shouldVisit(link, pageNode)) {
                crawlableLinks.add(link.getURL());
            }
        }

        try {
            graphImporter.addLinks(pageNode, crawlableLinks);
        } catch (Exception e) {
            logger.error("Error creating " + crawlableLinks.size() + " links for " + PageNode.getUrl(pageNode), e);
        }
    }

    private void visitNonHtmlPage(String url) {
        try {
            graphImporter.addPage(url, "");
        } catch (Exception e) {
            logger.error("Error creating (non-HTML) node for " + url, e);
        }
    }
}
