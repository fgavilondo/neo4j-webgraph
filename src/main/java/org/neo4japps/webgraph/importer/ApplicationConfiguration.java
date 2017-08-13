package org.neo4japps.webgraph.importer;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.net.MalformedURLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;
import java.util.MissingResourceException;
import java.util.Properties;
import java.util.Set;

import org.neo4japps.webgraph.util.UrlUtil;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

public class ApplicationConfiguration {

    public static final String DEFAULT_DB_DIR_LOCATION = "./graph.db";
    public static final String DEFAULT_CRAWL_DATA_ROOT = "./crawl-data/root";

    public static final String DEFAULT_CONFIG_FILE_NAME = "config.properties";

    public static final String ROOT_URL_KEY = "rootUrl";
    public static final String SEED_URLS_KEY = "seedUrls";
    public static final String DOMAINS_TO_CRAWL_KEY = "domainsToCrawl";
    public static final String SUBDOMAINS_TO_IGNORE_KEY = "subdomainsToIgnore";
    public static final String EVENT_HANDLERS_KEY = "customEventHandlerClasses";

    private static final OptionParser OPTIONS_PARSER = new OptionParser() {
        {
            accepts("r").withOptionalArg().ofType(Boolean.class).describedAs(
                    "resumable import ((defaults to false , i.e start afresh and discard all previously imported data)");
            accepts("b").withOptionalArg().ofType(Boolean.class)
                    .describedAs("include binary content in crawling (defaults to false)");
            accepts("s").withOptionalArg().ofType(Boolean.class)
                    .describedAs("include HTTPS pages in crawling (defaults to false)");
            accepts("t").withOptionalArg().ofType(Boolean.class)
                    .describedAs("Use DB transactions during import (defaults to false, i.e. batch import)");

            accepts("c").withRequiredArg().ofType(Integer.class).describedAs("number of crawler threads").defaultsTo(2);
            accepts("f").withRequiredArg().ofType(Integer.class).describedAs("max pages to fetch").defaultsTo(-1);
            accepts("d").withRequiredArg().ofType(Integer.class).describedAs("max depth of crawling").defaultsTo(10);
            accepts("p").withRequiredArg().ofType(Integer.class).describedAs("politeness delay").defaultsTo(50);
            accepts("rf").withRequiredArg().ofType(Integer.class).describedAs("import progress report frequency")
                    .defaultsTo(500);
            accepts("ts").withRequiredArg().ofType(Integer.class)
                    .describedAs("transaction size (number of nodes to import per DB transaction)").defaultsTo(500);

            accepts("proxyHost").withRequiredArg().ofType(String.class).describedAs("proxy host");
            accepts("proxyPort").withRequiredArg().ofType(Integer.class).describedAs("proxy port").defaultsTo(8080);
            accepts("proxyUsername").withRequiredArg().ofType(String.class).describedAs("proxy user name");
            accepts("proxyPassword").withRequiredArg().ofType(String.class).describedAs("proxy password");

            accepts("config").withRequiredArg().ofType(String.class).describedAs("configuration file name")
                    .defaultsTo(DEFAULT_CONFIG_FILE_NAME);

            acceptsAll(Arrays.asList("h", "?"), "show help");
        }
    };

    private final OptionSet cliOptions;

    private final boolean resumableImport;
    private final boolean includeBinaryContentInCrawling;
    private final boolean includeHttpsPages;

    private final int numberOfCrawlers;
    private final int maxPagesToFetch; // crawler default is -1 (unlimited)
    private final int maxDepthOfCrawling; // crawler default is -1 (unlimited)
    private final int politenessDelay; // crawler default is 200
    private final int importProgressReportFrequency;

    private final String proxyHost; // e.g. "proxy1.mydomain.net"
    private final int proxyPort;
    private final String proxyUsername;
    private final String proxyPassword;

    private final boolean useTransactions;
    private final int transactionSize;

    private final String configFileName;
    private final String configFileFullPath;
    private final Properties configProperties;

    // TODO make these configurable via command line
    private final int maxConnectionsPerHost = 700; // hard-coded for now
    private final int maxTotalConnections = 700; // hard-coded for now
    private final boolean respectRobotsTxt = false; // hard-coded for now
    private final String dbDir = DEFAULT_DB_DIR_LOCATION; // hard-coded for now
    private final String crawlStorageDir = DEFAULT_CRAWL_DATA_ROOT; // hard-coded
                                                                    // for now

    public ApplicationConfiguration(String[] cmdLineArgs) throws Exception {
        this(cmdLineArgs, null);
    }

    /**
     * Constructor for unit tests so that we can use mock properties
     */
    public ApplicationConfiguration(String[] cmdLineArgs, Properties props) throws Exception {
        cliOptions = OPTIONS_PARSER.parse(cmdLineArgs == null ? new String[] {} : cmdLineArgs);

        resumableImport = getOptionalBooleanArgument("r", false);
        includeBinaryContentInCrawling = getOptionalBooleanArgument("b", false);
        includeHttpsPages = getOptionalBooleanArgument("s", false);

        numberOfCrawlers = (Integer) cliOptions.valueOf("c");
        if (numberOfCrawlers < 1) {
            throw new IllegalArgumentException("Value for option 'c' must be >= 1");
        }

        maxPagesToFetch = (Integer) cliOptions.valueOf("f");
        if (maxPagesToFetch < 1 && maxPagesToFetch != -1) {
            throw new IllegalArgumentException(
                    "Value for option 'f' must be >= 1, or -1 for unlimited number of pages");
        }

        maxDepthOfCrawling = (Integer) cliOptions.valueOf("d");
        if (maxDepthOfCrawling < 1 && maxDepthOfCrawling != -1) {
            throw new IllegalArgumentException("Value for option 'd' must be >= 1, or -1 for unlimited depth");
        }

        politenessDelay = (Integer) cliOptions.valueOf("p");
        if (politenessDelay < 0) {
            throw new IllegalArgumentException("Value for option 'p' must be >= 0 milliseconds");
        }

        importProgressReportFrequency = (Integer) cliOptions.valueOf("rf");
        if (importProgressReportFrequency < 1) {
            throw new IllegalArgumentException("Value for option 'rf' must be >= 1 node(s)");
        }

        useTransactions = getOptionalBooleanArgument("t", false);

        transactionSize = (Integer) cliOptions.valueOf("ts");
        if (transactionSize < 1) {
            throw new IllegalArgumentException("Value for option 'ts' must be >= 1 node(s)");
        }

        proxyHost = (String) cliOptions.valueOf("proxyHost");
        proxyPort = (Integer) cliOptions.valueOf("proxyPort");
        if (proxyPort < 1) {
            throw new IllegalArgumentException("Value for option 'proxyPort' must be >= 1");
        }
        proxyUsername = (String) cliOptions.valueOf("proxyUsername");
        proxyPassword = (String) cliOptions.valueOf("proxyPassword");

        configFileName = (String) cliOptions.valueOf("config");
        if (props == null) {
            // normal case
            configProperties = new Properties();
            configFileFullPath = loadProperties();
        } else {
            // unit tests
            configProperties = props;
            configFileFullPath = configFileName;
        }
    }

    private String loadProperties() throws IOException {
        String path;

        try {
            // first try to load the file from the file system
            InputStream inStream = new FileInputStream(configFileName);
            path = new File(configFileName).getPath();
            configProperties.load(inStream);
            try {
                inStream.close();
            } catch (IOException ignore) {
            }
        } catch (FileNotFoundException e) {
            // if not found, try to read it as a classpath resource
            InputStream inStream = this.getClass().getClassLoader().getResourceAsStream(configFileName);
            if (inStream == null) {
                throw new IOException("Could not find config file: " + configFileName);
            }
            path = this.getClass().getClassLoader().getResource(configFileName).getPath();
            configProperties.load(inStream);
            try {
                inStream.close();
            } catch (IOException ignore) {
            }
        }

        return path;
    }

    private boolean getOptionalBooleanArgument(String option, boolean defaultValue) {
        if (!cliOptions.has(option)) {
            return defaultValue;
        }

        if (!cliOptions.hasArgument(option)) {
            return !defaultValue;
        }

        return (Boolean) cliOptions.valueOf(option);
    }

    public static void printHelpOn(PrintStream sink) throws IOException {
        if (sink != null) {
            OPTIONS_PARSER.printHelpOn(sink);
        }
    }

    public boolean isHelp() {
        return cliOptions.has("h") || cliOptions.has("?");
    }

    public List<String> nonOptionArguments() {
        return cliOptions.nonOptionArguments();
    }

    public void dumpOn(PrintStream sink) {
        sink.println("Command line options:");
        sink.println();
        sink.println("resumableImport: " + isResumableImport());
        sink.println("includeBinaryContentInCrawling: " + isIncludeBinaryContentInCrawling());
        sink.println("includeHttpsPages: " + isIncludeHttpsPages());
        sink.println("numberOfCrawlers: " + getNumberOfCrawlers());
        sink.println("maxPagesToFetch: " + getMaxPagesToFetch());
        sink.println("maxDepthOfCrawling: " + getMaxDepthOfCrawling());
        sink.println("politenessDelay: " + getPolitenessDelay() + " ms");
        sink.println("importProgressReportFrequency every: " + getImportProgressReportFrequency() + " nodes");

        sink.println("proxyHost: " + getProxyHost());
        sink.println("proxyPort: " + getProxyPort());
        sink.println("proxyUsername: " + getProxyUsername());
        sink.println("proxyPassword: " + mask(getProxyPassword(), '*'));

        sink.println("");
        sink.println("useTransactions: " + isUseTransactions());
        if (isUseTransactions()) {
            // only display this if using the transactional importer, otherwise
            // confusing
            sink.println("transactionSize: " + getTransactionSize() + " nodes");
        } else {
            sink.println("numberOfBatchImporterThreads: " + getNumberOfBatchImporterThreads());
        }

        sink.println("");
        sink.println("respectRobotsTxt: " + isRespectRobotsTxt() + " (hard-coded)");
        sink.println("maxConnectionsPerHost: " + getMaxConnectionsPerHost() + " (hard-coded)");
        sink.println("maxTotalConnections: " + getMaxTotalConnections() + " (hard-coded)");
        sink.println("databaseDirectory: " + getDbDir() + " (hard-coded)");
        sink.println("crawlStorageDir: " + getCrawlStorageDir() + " (hard-coded)");
        sink.println();
        sink.println("Properties (loaded from " + configFileFullPath + "):");
        sink.println();
        Set<Entry<Object, Object>> entrySet = configProperties.entrySet();
        for (Entry<Object, Object> entry : entrySet) {
            sink.println(entry.getKey() + ": " + entry.getValue());
            sink.println();
        }
    }

    private String mask(String string, Character maskCharacter) {
        if (string == null) {
            return null;
        }
        String mask = "";
        for (int i = 0; i < string.length(); i++) {
            mask += maskCharacter;

        }
        return mask;
    }

    public boolean isResumableImport() {
        return resumableImport;
    }

    public boolean isIncludeBinaryContentInCrawling() {
        return includeBinaryContentInCrawling;
    }

    public boolean isIncludeHttpsPages() {
        return includeHttpsPages;
    }

    public boolean isRespectRobotsTxt() {
        return respectRobotsTxt;
    }

    public int getNumberOfCrawlers() {
        return numberOfCrawlers;
    }

    public int getNumberOfBatchImporterThreads() {
        return isUseTransactions() ? 0 : getNumberOfCrawlers() * 20;
    }

    public int getMaxPagesToFetch() {
        return maxPagesToFetch;
    }

    public int getMaxDepthOfCrawling() {
        return maxDepthOfCrawling;
    }

    public int getPolitenessDelay() {
        return politenessDelay;
    }

    public int getImportProgressReportFrequency() {
        return importProgressReportFrequency;
    }

    public String getProxyHost() {
        return proxyHost;
    }

    public int getProxyPort() {
        return proxyPort;
    }

    public String getProxyUsername() {
        return proxyUsername;
    }

    public String getProxyPassword() {
        return proxyPassword;
    }

    public boolean isUseTransactions() {
        return useTransactions;
    }

    public int getTransactionSize() {
        return transactionSize;
    }

    public int getMaxConnectionsPerHost() {
        return maxConnectionsPerHost;
    }

    public int getMaxTotalConnections() {
        return maxTotalConnections;
    }

    public String getDbDir() {
        return dbDir;
    }

    public String getCrawlStorageDir() {
        return crawlStorageDir;
    }

    public String getConfigFileName() {
        return configFileName;
    }

    public String getConfigFileFullPath() {
        return configFileFullPath;
    }

    public String getRootUrl() {
        String property = configProperties.getProperty(ROOT_URL_KEY);
        if (property == null) {
            throw new MissingResourceException("Missing configuration property " + ROOT_URL_KEY, getConfigFileName(),
                    ROOT_URL_KEY);
        }
        return property.trim();
    }

    public String[] getSeedUrls() {
        return getCommaSeparatedPropertyValues(SEED_URLS_KEY);
    }

    public String[] getDomainsToCrawl() {
        return getCommaSeparatedPropertyValues(DOMAINS_TO_CRAWL_KEY);
    }

    public String[] getSubdomainsToIgnore() {
        return getCommaSeparatedPropertyValues(SUBDOMAINS_TO_IGNORE_KEY);
    }

    public String[] getCustomEventHandlerClasses() {
        return getCommaSeparatedPropertyValues(EVENT_HANDLERS_KEY);
    }

    private String[] getCommaSeparatedPropertyValues(String key) {
        String property = configProperties.getProperty(key);
        if (property == null) {
            throw new MissingResourceException("Missing configuration property " + key, getConfigFileName(), key);
        }

        property = property.trim();
        if (property.length() == 0) {
            return new String[0];
        }

        String[] values = property.split(",");
        for (int i = 0; i < values.length; i++) {
            values[i] = values[i].trim();
        }
        return values;
    }

    public boolean isCrawlableUrl(String url) {
        if (url == null) {
            return false;
        }
        return isCrawlableDomain(url) && !isExcludedSubdomain(url);
    }

    private boolean isCrawlableDomain(String url) {
        return urlMatchesAtLeastOneDomain(url, getDomainsToCrawl());
    }

    private boolean isExcludedSubdomain(String url) {
        return urlMatchesAtLeastOneDomain(url, getSubdomainsToIgnore());
    }

    private boolean urlMatchesAtLeastOneDomain(String url, String[] domains) {
        try {
            String host = UrlUtil.extractHost(url);
            for (int i = 0; i < domains.length; i++) {
                if (host.contains(domains[i])) {
                    return true;
                }
            }
            return false;
        } catch (MalformedURLException e) {
            return false;
        }
    }

    void confirm(PrintStream sink, InputStream inputStream) throws IOException {
        sink.println();
        sink.println("Configuration:");
        sink.println();

        dumpOn(sink);

        sink.println();
        sink.print("Press ENTER to continue or Ctrl-C to quit...");
        inputStream.read();
    }
}
