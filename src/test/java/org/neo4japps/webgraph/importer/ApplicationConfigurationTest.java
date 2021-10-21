package org.neo4japps.webgraph.importer;

import joptsimple.OptionException;
import junit.framework.TestCase;

import java.io.IOException;
import java.util.MissingResourceException;
import java.util.Properties;

public class ApplicationConfigurationTest extends TestCase {

    public void testThatDefaultConfigurationIsCorrect() throws Exception {
        ApplicationConfiguration appConfig = new ApplicationConfiguration(new String[]{}, new Properties());
        assertDefaultConfig(appConfig);
    }

    private void assertDefaultConfig(ApplicationConfiguration appConfig) {
        assertFalse(appConfig.isResumableImport());
        assertFalse(appConfig.isIncludeBinaryContentInCrawling());
        assertFalse(appConfig.isIncludeHttpsPages());

        assertEquals(2, appConfig.getNumberOfCrawlers());
        assertEquals(-1, appConfig.getMaxPagesToFetch());
        assertEquals(10, appConfig.getMaxDepthOfCrawling());
        assertEquals(50, appConfig.getPolitenessDelay());
        assertEquals(500, appConfig.getImportProgressReportFrequency());

        assertNull(appConfig.getProxyHost());
        assertEquals(8080, appConfig.getProxyPort());
        assertNull(appConfig.getProxyUsername());
        assertNull(appConfig.getProxyPassword());

        assertFalse(appConfig.isUseTransactions());
        assertEquals(500, appConfig.getTransactionSize());

        assertEquals(700, appConfig.getMaxConnectionsPerHost());
        assertEquals(700, appConfig.getMaxTotalConnections());
        assertFalse(appConfig.isRespectRobotsTxt());
        assertEquals("./graph.db", appConfig.getDbDir());
        assertEquals("./crawl-data/root", appConfig.getCrawlStorageDir());

        assertFalse(appConfig.isHelp());

        assertEquals(ApplicationConfiguration.DEFAULT_CONFIG_FILE_NAME, appConfig.getConfigFileName());
    }

    public void testThatNullConfigurationIsTheSameAsDefaultConfiguration() throws Exception {
        ApplicationConfiguration appConfig = new ApplicationConfiguration(null, new Properties());
        assertDefaultConfig(appConfig);
    }

    public void testThatUnrecognizedOptionsAreRejected() throws Exception {
        try {
            new ApplicationConfiguration(new String[]{"-x"}, new Properties());
            fail("Expected UnrecognizedOptionException");
        } catch (OptionException expected) {
            assertEquals("x is not a recognized option", expected.getMessage());
        }
    }

    public void testThatHelpOptionsAreRecognized() throws Exception {
        ApplicationConfiguration appConfig = new ApplicationConfiguration(new String[]{"-h"}, new Properties());
        assertTrue(appConfig.isHelp());
        appConfig = new ApplicationConfiguration(new String[]{"-?"}, new Properties());
        assertTrue(appConfig.isHelp());
    }

    public void testOptionsWithOptionalArguments() throws Exception {
        ApplicationConfiguration appConfig = new ApplicationConfiguration(new String[]{"-r", "-b", "-s", "-t"},
                new Properties());
        assertTrue(appConfig.isResumableImport());
        assertTrue(appConfig.isIncludeBinaryContentInCrawling());
        assertTrue(appConfig.isIncludeHttpsPages());
        assertTrue(appConfig.isUseTransactions());

        appConfig = new ApplicationConfiguration(
                new String[]{"-r", "true", "-b", "true", "-s", "true", "-t", "true"}, new Properties());
        assertTrue(appConfig.isResumableImport());
        assertTrue(appConfig.isIncludeBinaryContentInCrawling());
        assertTrue(appConfig.isIncludeHttpsPages());
        assertTrue(appConfig.isUseTransactions());

        appConfig = new ApplicationConfiguration(
                new String[]{"-r", "false", "-b", "false", "-s", "false", "-t", "false"}, new Properties());
        assertFalse(appConfig.isResumableImport());
        assertFalse(appConfig.isIncludeBinaryContentInCrawling());
        assertFalse(appConfig.isIncludeHttpsPages());
        assertFalse(appConfig.isUseTransactions());
    }

    public void testOptionsWithRequiredArguments() throws Exception {
        doTestRequiredArgument("c");
        doTestRequiredArgument("f");
        doTestRequiredArgument("d");
        doTestRequiredArgument("p");
        doTestRequiredArgument("rf");
        doTestRequiredArgument("ts");

        doTestRequiredArgument("proxyHost");
        doTestRequiredArgument("proxyPort");
        doTestRequiredArgument("proxyUsername");
        doTestRequiredArgument("proxyPassword");

        doTestRequiredArgument("config");
    }

    private void doTestRequiredArgument(String option) throws Exception {
        try {
            new ApplicationConfiguration(new String[]{"-" + option}, new Properties());
            fail("Expected OptionMissingRequiredArgumentException");
        } catch (OptionException expected) {
            assertEquals("Option " + option + " requires an argument", expected.getMessage());
        }
    }

    public void testArgumentValidation() throws Exception {
        doTestZeroOrPositiveArgumentValueValidation("p");

        doTestPositiveArgumentValueValidation("c");
        doTestPositiveArgumentValueValidation("f");
        doTestPositiveArgumentValueValidation("d");
        doTestPositiveArgumentValueValidation("rf");
        doTestPositiveArgumentValueValidation("ts");
        doTestPositiveArgumentValueValidation("proxyPort");

        doTestUnlimitedArgumentValueValidation("f");
        doTestUnlimitedArgumentValueValidation("d");
    }

    private void doTestUnlimitedArgumentValueValidation(String argument) throws Exception {
        new ApplicationConfiguration(new String[]{"-" + argument, "-1"}, new Properties());
    }

    private void doTestZeroOrPositiveArgumentValueValidation(String argument) throws Exception {
        try {
            new ApplicationConfiguration(new String[]{"-" + argument, "-1"}, new Properties());
            fail("Expected IllegalArgumentException for parameter -" + argument + " -1");
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains("'" + argument + "'"));
        }
    }

    private void doTestPositiveArgumentValueValidation(String argument) throws Exception {
        try {
            new ApplicationConfiguration(new String[]{"-" + argument, "0"}, new Properties());
            fail("Expected IllegalArgumentException for parameter -" + argument + " 0");
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains("'" + argument + "'"));
        }
    }

    public void testThatIOExceptionIdThrownWhenConfigFileNotFound() throws Exception {
        final String fileName = "YouWontFindMe.properties";
        try {
            new ApplicationConfiguration(new String[]{"-config", fileName});
            fail("Expected IOException");
        } catch (IOException expected) {
            assertTrue(expected.getMessage().contains(fileName));
        }
    }

    public void testThatDefaultConfigFileCanBeFoundAndContainsAllNeededProperties() throws Exception {
        ApplicationConfiguration appConfig = new ApplicationConfiguration(new String[]{});
        assertTrue(appConfig.getConfigFileFullPath().contains(ApplicationConfiguration.DEFAULT_CONFIG_FILE_NAME));
        assertNotNull(appConfig.getRootUrl());
        assertNotNull(appConfig.getSeedUrls());
        assertNotNull(appConfig.getDomainsToCrawl());
        assertNotNull(appConfig.getSubdomainsToIgnore());
        assertNotNull(appConfig.getCustomEventHandlerClasses());
    }

    public void testThatConfigPropertiesAreReadCorrectly() throws Exception {
        Properties props = new Properties();
        props.setProperty(ApplicationConfiguration.ROOT_URL_KEY, "https://mycompany.com");
        props.setProperty(ApplicationConfiguration.SEED_URLS_KEY,
                "https://mycompany.com, https://sub1.mycompany.com, https://sub2.mycompany.com");
        props.setProperty(ApplicationConfiguration.DOMAINS_TO_CRAWL_KEY,
                "mycompany.com, mycomp.com, my.comp.com, my-compmany.com");
        props.setProperty(ApplicationConfiguration.SUBDOMAINS_TO_IGNORE_KEY, "shopping.mycompany.com");
        props.setProperty(ApplicationConfiguration.EVENT_HANDLERS_KEY,
                "org.neo4japps.webgraph.customhandlers.SomeEventHandler");

        ApplicationConfiguration appConfig = new ApplicationConfiguration(new String[]{}, props);

        assertEquals("https://mycompany.com", appConfig.getRootUrl());
        assertEquals(3, appConfig.getSeedUrls().length);
        assertEquals(4, appConfig.getDomainsToCrawl().length);
        assertEquals(1, appConfig.getSubdomainsToIgnore().length);
        String[] customEventHandlerClasses = appConfig.getCustomEventHandlerClasses();
        assertEquals(1, customEventHandlerClasses.length);

        assertTrue(appConfig.isCrawlableUrl("https://mycompany.com"));
        assertTrue(appConfig.isCrawlableUrl("https://mycompany.com/"));
        assertTrue(appConfig.isCrawlableUrl("https://mycompany.com/bla"));
        assertTrue(appConfig.isCrawlableUrl("https://bla1.mycompany.com/bla"));
        assertTrue(appConfig.isCrawlableUrl("https://www.mycomp.com.com/bla"));

        assertFalse(appConfig.isCrawlableUrl(null));
        assertFalse(appConfig.isCrawlableUrl(""));
        assertFalse(appConfig.isCrawlableUrl("https://shopping.mycompany.com/product1"));

        props.setProperty(ApplicationConfiguration.EVENT_HANDLERS_KEY, "");
        appConfig = new ApplicationConfiguration(new String[]{}, props);
        customEventHandlerClasses = appConfig.getCustomEventHandlerClasses();
        assertEquals(0, customEventHandlerClasses.length);

        props.setProperty(ApplicationConfiguration.EVENT_HANDLERS_KEY, "  ");
        appConfig = new ApplicationConfiguration(new String[]{}, props);
        customEventHandlerClasses = appConfig.getCustomEventHandlerClasses();
        assertEquals(0, customEventHandlerClasses.length);
    }

    public void testCrawlableUrlDeterminationGivenStandardConfiguration() throws Exception {
        ApplicationConfiguration appConfig = new ApplicationConfiguration(new String[]{});
        assertFalse(appConfig.isCrawlableUrl("https://login.live.com/"));
        assertFalse(appConfig.isCrawlableUrl(
                "https://login.live.com/login.srf?cb=area%3Dninemsn.com.au&ct=1203904351&id=76307&lc=1033&rpsnv=10&rver=4.5.2125.0&wa=wsignin1.0&wp=LBI&wreply=http%3A%2F%2Fninemsn.com.au"));
    }

    public void testThatExceptionIsThrownWhenPropertiesAreMissing() throws Exception {
        Properties props = new Properties();
        ApplicationConfiguration appConfig = new ApplicationConfiguration(new String[]{}, props);

        try {
            appConfig.getRootUrl();
            fail("Expected MissingResourceException");
        } catch (MissingResourceException expected) {
        }
        try {
            appConfig.getSeedUrls();
            fail("Expected MissingResourceException");
        } catch (MissingResourceException expected) {
        }
        try {
            appConfig.getDomainsToCrawl();
            fail("Expected MissingResourceException");
        } catch (MissingResourceException expected) {
        }
        try {
            appConfig.getSubdomainsToIgnore();
            fail("Expected MissingResourceException");
        } catch (MissingResourceException expected) {
        }
        try {
            appConfig.getCustomEventHandlerClasses();
            fail("Expected MissingResourceException");
        } catch (MissingResourceException expected) {
        }
    }
}
