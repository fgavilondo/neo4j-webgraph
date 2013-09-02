package org.neo4japps.webgraph.importer;

import java.io.IOException;
import java.text.DateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.MissingResourceException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4japps.webgraph.util.DirectoryUtil;
import org.neo4japps.webgraph.util.StringFormatUtil;

import edu.uci.ics.crawler4j.crawler.CrawlConfig;
import edu.uci.ics.crawler4j.crawler.CrawlController;
import edu.uci.ics.crawler4j.fetcher.PageFetcher;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtConfig;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtServer;

public class App {
    private final Set<GraphObserver> graphObservers = Collections.synchronizedSet(new HashSet<GraphObserver>());

    final Logger logger = Logger.getLogger(this.getClass());

    private final String[] args;
    private final boolean silent;

    private final AtomicBoolean isShutdown = new AtomicBoolean();

    private ApplicationConfiguration appConfig;
    private GraphImporter graphImporter = null;
    private boolean isSuccessfulImport = false;
    private long startTimeInMillis;

    public App(String[] args) {
        this(args, false);
    }

    /**
     * Package private visibility for unit testing.
     * 
     * Pass silent = true to skip printing help and other messages in tests.
     */
    App(String[] args, boolean silent) {
        this.args = args;
        this.silent = silent;
    }

    /**
     * Package private visibility for unit testing.
     * 
     * Pass silent = true to skip printing help and other messages in tests.
     */
    App(ApplicationConfiguration config, GraphImporter importer, boolean silent) {
        this.args = null;
        this.silent = silent;

        this.appConfig = config;
        this.graphImporter = importer;
    }

    public void run() throws Exception {
        if (!createConfig()) {
            return;
        }

        if (!checkConfigOnlyHasRecognizedOptions()) {
            return;
        }

        if (checkConfigForHelp()) {
            return;
        }

        if (!silent) {
            appConfig.confirm(System.out, System.in);
        }

        if (!appConfig.isResumableImport()) {
            deleteEmbeddedDatabaseDir();
        }

        startTimeInMillis = System.currentTimeMillis();
        if (!silent) {
            logger.info("Started import " + DateFormat.getDateTimeInstance().format(new Date(startTimeInMillis)));
        }

        doRun();

        shutdown(false);
    }

    synchronized void shutdown(boolean isQuickShutdown) {
        // ensure we only go through this code once, regardless of how the VM
        // terminated
        if (isShutdown.get()) {
            return;
        }

        isShutdown.set(true);

        if (isQuickShutdown) {
            quickShutdown();
        } else {
            normalShutdown();
        }

        String shutdownType = isQuickShutdown ? "quick" : "normal";
        logger.info("Finished " + shutdownType + " shutdown sequence.");
    }

    private void quickShutdown() {
        if (!graphObservers.isEmpty()) {
            System.out.println("Shutting down graph event handlers");
            for (GraphObserver observer : graphObservers) {
                observer.shutdown();
            }
        }

        if (graphImporter != null) {
            System.out.println("Shutting down database");
            graphImporter.shutdown();
        }
    }

    private void normalShutdown() {
        logger.info("Starting shutdown sequence.");

        if (graphImporter != null) {
            logger.info("Stopping import");
            graphImporter.stop();
        }

        if (!graphObservers.isEmpty()) {
            logger.info("Shutting down graph event handlers");
            for (GraphObserver observer : graphObservers) {
                observer.shutdown();
            }
        }

        reportResults();

        if (graphImporter != null) {
            logger.info("Shutting down database");
            graphImporter.shutdown();
        }
    }

    private boolean createConfig() throws IOException {
        if (appConfig != null) {
            // don't create twice
            return true;
        }

        try {
            appConfig = new ApplicationConfiguration(args);
            return true;
        } catch (Exception e) {
            if (!silent) {
                System.out.println(e.getMessage());
                ApplicationConfiguration.printHelpOn(System.out);
            }
            return false;
        }
    }

    private boolean checkConfigOnlyHasRecognizedOptions() throws IOException {
        List<String> nonOptionArguments = appConfig.nonOptionArguments();
        if (nonOptionArguments.isEmpty()) {
            return true;
        }
        if (!silent) {
            System.out.println("'" + nonOptionArguments.get(0) + "' is not a recognized option");
            ApplicationConfiguration.printHelpOn(System.out);
        }
        return false;
    }

    private boolean checkConfigForHelp() throws IOException {
        if (appConfig.isHelp()) {
            if (!silent) {
                ApplicationConfiguration.printHelpOn(System.out);
            }
            return true;
        }
        return false;
    }

    private void deleteEmbeddedDatabaseDir() throws IOException {
        DirectoryUtil.deleteDir(appConfig.getDbDir());
        if (!silent) {
            logger.info("Deleted database directory " + appConfig.getDbDir());
        }
    }

    private void doRun() throws Exception {
        createGraphImporter();

        registerShutdownHook();
        registerCustomEventHandlers();

        crawlAndImport();

        graphImporter.waitForImportToFinish();

        isSuccessfulImport = true;
    }

    private void createGraphImporter() {
        if (graphImporter != null) {
            // don't create twice
            return;
        }

        if (appConfig.isUseTransactions()) {
            GraphDatabaseService graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(appConfig.getDbDir());
            graphImporter = new TransactionalGraphImporter(graphDb, appConfig.getRootUrl(), startTimeInMillis,
                    appConfig.getImportProgressReportFrequency(), appConfig.getTransactionSize());
        } else {
            graphImporter = new BatchGraphImporter(appConfig.getDbDir(), appConfig.getRootUrl(), startTimeInMillis,
                    appConfig.getImportProgressReportFrequency(), appConfig.getNumberOfBatchImporterThreads());
        }
    }

    private void registerShutdownHook() {
        // Registers a shutdown hook for the Application so that it
        // shuts down nicely when the VM exits (even if you "Ctrl-C" the
        // running application before it's completed)
        Thread shutdownHook = new Thread() {
            @Override
            public void run() {
                try {
                    shutdown(false);
                } catch (Exception e) {
                    logger.warn("Shutdown error", e);
                }
            }
        };

        Runtime.getRuntime().addShutdownHook(shutdownHook);
    }

    private void registerCustomEventHandlers() {
        String[] classNames;
        try {
            classNames = appConfig.getCustomEventHandlerClasses();
        } catch (MissingResourceException e) {
            logger.info("No custom event handler classes configured");
            return;
        }

        for (int i = 0; i < classNames.length; i++) {
            String className = classNames[i];
            Class<GraphObserver> clazz;
            try {
                clazz = (Class<GraphObserver>) Class.forName(className);
                GraphObserver graphObserver;
                try {
                    graphObserver = clazz.newInstance();
                    ((ConcurrentObservable) graphImporter).addObserver(graphObserver);
                    graphObserver.configure(appConfig);
                    graphObservers.add(graphObserver);
                } catch (InstantiationException | IllegalAccessException e) {
                    logger.warn("Error creating instance of '" + className + "'. Ignoring.", e);
                }
            } catch (ClassNotFoundException e) {
                logger.warn("Could not find class '" + className + "'. Ignoring.");
            }
        }
    }

    /**
     * This is where everything happens!
     */
    private void crawlAndImport() throws Exception {

        CrawlConfig crawlConfig = buildCrawlConfig();
        PageFetcher pageFetcher = new PageFetcher(crawlConfig);
        RobotstxtConfig robotsTxtConfig = new RobotstxtConfig();
        robotsTxtConfig.setEnabled(appConfig.isRespectRobotsTxt());
        RobotstxtServer robotsTxtServer = new RobotstxtServer(robotsTxtConfig, pageFetcher);
        CrawlController crawlController = new CrawlController(crawlConfig, pageFetcher, robotsTxtServer);

        // "dependency injection" into crawlers
        Object[] customData = new Object[] { appConfig, graphImporter };
        crawlController.setCustomData(customData);

        addSeedUrls(crawlController);

        logger.info("Start crawling");

        /*
         * Start the crawl. This is a blocking operation, meaning that your code
         * will reach the line after this only when crawling is finished.
         */
        crawlController.start(HtmlOnlyCrawler.class, appConfig.getNumberOfCrawlers());

        logger.info("Finished crawling");
    }

    private CrawlConfig buildCrawlConfig() {
        CrawlConfig crawlConfig = new CrawlConfig();

        crawlConfig.setCrawlStorageFolder(appConfig.getCrawlStorageDir());
        crawlConfig.setIncludeBinaryContentInCrawling(appConfig.isIncludeBinaryContentInCrawling());
        crawlConfig.setIncludeHttpsPages(appConfig.isIncludeHttpsPages());

        /*
         * Be polite (200 ms min).
         */
        crawlConfig.setPolitenessDelay(appConfig.getPolitenessDelay());

        /*
         * You can set the maximum crawl depth here. The default value is -1 for
         * unlimited depth
         */
        crawlConfig.setMaxDepthOfCrawling(appConfig.getMaxDepthOfCrawling());

        /*
         * You can set the maximum number of pages to crawl. The default value
         * is -1 for unlimited number of pages.
         */
        crawlConfig.setMaxPagesToFetch(appConfig.getMaxPagesToFetch());

        /*
         * This config parameter can be used to set your crawl to be resumable
         * (meaning that you can resume the crawl from a previously
         * interrupted/crashed crawl). Note: if you enable resuming feature and
         * want to start a fresh crawl, you need to delete the contents of
         * rootFolder manually.
         */
        crawlConfig.setResumableCrawling(appConfig.isResumableImport());

        crawlConfig.setMaxConnectionsPerHost(appConfig.getMaxConnectionsPerHost());
        crawlConfig.setMaxTotalConnections(appConfig.getMaxTotalConnections());

        if (appConfig.getProxyHost() != null) {
            crawlConfig.setProxyHost(appConfig.getProxyHost());
            crawlConfig.setProxyPort(appConfig.getProxyPort());
        }
        if (appConfig.getProxyUsername() != null) {
            crawlConfig.setProxyUsername(appConfig.getProxyUsername());
        }
        if (appConfig.getProxyPassword() != null) {
            crawlConfig.setProxyPassword(appConfig.getProxyPassword());
        }

        return crawlConfig;
    }

    /**
     * For each crawl, we need to add some seed URLs. These are the first URLs
     * that are fetched and then the crawler starts following links which are
     * found in these pages.
     */
    private void addSeedUrls(CrawlController crawlController) {
        String[] seedUrls = appConfig.getSeedUrls();
        for (int i = 0; i < seedUrls.length; i++) {
            crawlController.addSeed(seedUrls[i]);
        }
    }

    private void reportResults() {
        if (!silent) {
            final long endMillis = System.currentTimeMillis();
            final double elapsedSeconds = (endMillis - startTimeInMillis) / 1000;
            logger.info("");
            logger.info("Finished importing at " + DateFormat.getDateTimeInstance().format(new Date(endMillis)));
            logger.info("Elapsed time: " + StringFormatUtil.formatSeconds(elapsedSeconds) + " secs ("
                    + StringFormatUtil.formatMinutes(elapsedSeconds / 60) + " min).");

            displayBasicGraphInfo(elapsedSeconds);
            displayFailedUrls();
        }
    }

    private void displayFailedUrls() {
        FailedUrls.getInstance().report(logger, 20);
    }

    /**
     * This method accesses the graph, so it must be called before the shutdown.
     */
    private void displayBasicGraphInfo(double elapsedSeconds) {
        if (graphImporter == null)
            return;

        logger.info("");
        logger.info("Basic Graph Info:");

        int numberOfCreatedNodes = graphImporter.getNumberOfPageNodes();
        int numberOfLinks = graphImporter.getNumberOfLinks();

        // avoid division by zero!
        final double nodesPerSecond = (elapsedSeconds == 0.00) ? numberOfCreatedNodes
                : (numberOfCreatedNodes / elapsedSeconds);
        logger.info("Nodes imported: " + numberOfCreatedNodes + ". That's "
                + StringFormatUtil.formatNodesPerSecond(nodesPerSecond) + " nodes per sec.");

        logger.info("Number of links between page nodes: " + numberOfLinks);

        displayTransactions();

        Node rootPage = graphImporter.getRootPage();
        Object rootPageUrl = rootPage == null ? "" : PageNode.getUrl(rootPage);
        logger.info("Root page: " + rootPageUrl);

        displayHomePages();

        logger.info("");
        reportObserverInfo();
    }

    private void displayTransactions() {
        if (graphImporter instanceof TransactionalGraphImporter) {
            int retriedTransactions = ((TransactionalGraphImporter) graphImporter).getNumberOfRetriedTransactions()
                    + getNumberOfRetriedObserverTransactions();
            logger.info("Retried transactions: " + retriedTransactions);

            int failedTransactions = ((TransactionalGraphImporter) graphImporter).getNumberOfFailedTransactions()
                    + getNumberOfFailedObserverTransactions();
            logger.info("Failed transactions: " + failedTransactions);
        }
    }

    private void displayHomePages() {
        int numberOfHomePages = graphImporter.getNumberOfPagesOfType(PageNode.HOME_PAGE);
        if (numberOfHomePages > 0) {
            logger.info(numberOfHomePages + " home pages:");
            Iterator<Node> homes = graphImporter.getAllPagesOfType(PageNode.HOME_PAGE);
            if (homes != null) {
                while (homes.hasNext()) {
                    logger.info("  " + PageNode.getUrl(homes.next()));
                }
            }
        }
    }

    private void reportObserverInfo() {
        for (GraphObserver observer : graphObservers) {
            final String observerClassName = observer.getClass().getSimpleName();
            logger.info(observerClassName + " received: " + observer.getNumberOfReceivedEvents() + " events");
            logger.info(observerClassName + " notified of: " + observer.getNumberOfNotifiedPageNodes() + " page nodes");
            logger.info(observerClassName + " updated: " + observer.getNumberOfUpdatedPageNodes() + " page nodes");
            logger.info(observerClassName + " ignored: " + observer.getNumberOfIgnoredPageNodes() + " page nodes");
            logger.info(observerClassName + " failed to update: " + observer.getNumberOfFailedUpdates() + " page nodes");

            if (observer instanceof CachingObserver && ((CachingObserver) observer).isCachingEnabled()) {
                logger.info(observerClassName + " cache hits: " + ((CachingObserver) observer).getCacheHits());
                logger.info(observerClassName + " cache misses: " + ((CachingObserver) observer).getCacheMisses());
                logger.info(observerClassName + " cache updates: " + ((CachingObserver) observer).getCacheUpdates());
            }

            logger.info("");
        }
    }

    private int getNumberOfFailedObserverTransactions() {
        int res = 0;
        for (GraphObserver observer : graphObservers) {
            res += observer.getNumberOfFailedTransactions();
        }
        return res;
    }

    private int getNumberOfRetriedObserverTransactions() {
        int res = 0;
        for (GraphObserver observer : graphObservers) {
            res += observer.getNumberOfRetriedTransactions();
        }
        return res;
    }

    public boolean isSuccessfulImport() {
        return isSuccessfulImport;
    }
}
