package org.neo4japps.webgraph.importer;

import edu.uci.ics.crawler4j.crawler.Page;
import edu.uci.ics.crawler4j.url.WebURL;
import junit.framework.TestCase;

import java.util.Properties;

public class HtmlOnlyCrawlerTest extends TestCase {
    private HtmlOnlyCrawler crawler;

    @Override
    protected void setUp() throws Exception {

        Properties props = new Properties();
        props.setProperty(ApplicationConfiguration.ROOT_URL_KEY, "https://my.company.com");
        props.setProperty(ApplicationConfiguration.SEED_URLS_KEY,
                "https://my.company.com, https://sub1.my.company.com, https://sub2.my.company.com");
        props.setProperty(ApplicationConfiguration.DOMAINS_TO_CRAWL_KEY,
                "my.company.com, mycomp.com, my.comp.com, my-compmany.com");
        props.setProperty(ApplicationConfiguration.SUBDOMAINS_TO_IGNORE_KEY, "shopping.my.company.com");

        ApplicationConfiguration appConfig = new ApplicationConfiguration(new String[]{}, props);

        HtmlOnlyCrawler.resetGlobalPageCount();
        crawler = new HtmlOnlyCrawler();
        crawler.setAppConfig(appConfig);
    }

    public void testUrlsToVisitAreFilteredCorrectly() {
        WebURL url = new WebURL();

        url.setURL("https://my.company.com/");
        assertTrue(crawler.shouldVisit(url));
        url.setURL("https://mycomp.com/");
        assertTrue(crawler.shouldVisit(url));
        url.setURL("https://my.company.com/somepage.html");
        assertTrue(crawler.shouldVisit(url));
        url.setURL("https://my.company.com/somepage.htm");
        assertTrue(crawler.shouldVisit(url));
        url.setURL("https://my.company.com/somepage.aspx");
        assertTrue(crawler.shouldVisit(url));
        url.setURL("https://sub1.my.company.com/");
        assertTrue(crawler.shouldVisit(url));
        url.setURL("https://sub2.my.company.com/");
        assertTrue(crawler.shouldVisit(url));
        url.setURL("https://sub3.my.company.com/");
        assertTrue(crawler.shouldVisit(url));

        // sub domain to ignore
        url.setURL("https://shopping.my.company.com/");
        assertFalse(crawler.shouldVisit(url));

        doTestMediaUrlsAreIgnored();

        doTestIncorrectMediaUrlsAreIgnored();

        // nothing should have been crawled
        assertEquals(0, HtmlOnlyCrawler.getGlobalPageCount());
    }

    private void doTestMediaUrlsAreIgnored() {
        WebURL url = new WebURL();

        url.setURL("https://my.company.com/somepage.css");
        assertFalse(crawler.shouldVisit(url));
        url.setURL("https://my.company.com/somepage.js");
        assertFalse(crawler.shouldVisit(url));
        url.setURL("https://my.company.com/somepage.bmp");
        assertFalse(crawler.shouldVisit(url));
        url.setURL("https://my.company.com/somepage.gif");
        assertFalse(crawler.shouldVisit(url));
        url.setURL("https://my.company.com/somepage.jpg");
        assertFalse(crawler.shouldVisit(url));
        url.setURL("https://my.company.com/somepage.jpeg");
        assertFalse(crawler.shouldVisit(url));
        url.setURL("https://my.company.com/somepage.png");
        assertFalse(crawler.shouldVisit(url));
        url.setURL("https://my.company.com/somepage.ico");
        assertFalse(crawler.shouldVisit(url));
        url.setURL("https://my.company.com/somepage.tif");
        assertFalse(crawler.shouldVisit(url));
        url.setURL("https://my.company.com/somepage.tiff");
        assertFalse(crawler.shouldVisit(url));
        url.setURL("https://my.company.com/somepage.mid");
        assertFalse(crawler.shouldVisit(url));
        url.setURL("https://my.company.com/somepage.mp2");
        assertFalse(crawler.shouldVisit(url));
        url.setURL("https://my.company.com/somepage.mp3");
        assertFalse(crawler.shouldVisit(url));
        url.setURL("https://my.company.com/somepage.mp4");
        assertFalse(crawler.shouldVisit(url));
        url.setURL("https://my.company.com/somepage.wav");
        assertFalse(crawler.shouldVisit(url));
        url.setURL("https://my.company.com/somepage.avi");
        assertFalse(crawler.shouldVisit(url));
        url.setURL("https://my.company.com/somepage.mov");
        assertFalse(crawler.shouldVisit(url));
        url.setURL("https://my.company.com/somepage.mpeg");
        assertFalse(crawler.shouldVisit(url));
        url.setURL("https://my.company.com/somepage.ram");
        assertFalse(crawler.shouldVisit(url));
        url.setURL("https://my.company.com/somepage.m4v");
        assertFalse(crawler.shouldVisit(url));
        url.setURL("https://my.company.com/somepage.pdf");
        assertFalse(crawler.shouldVisit(url));
        url.setURL("https://my.company.com/somepage.rm");
        assertFalse(crawler.shouldVisit(url));
        url.setURL("https://my.company.com/somepage.smil");
        assertFalse(crawler.shouldVisit(url));
        url.setURL("https://my.company.com/somepage.wmv");
        assertFalse(crawler.shouldVisit(url));
        url.setURL("https://my.company.com/somepage.swf");
        assertFalse(crawler.shouldVisit(url));
        url.setURL("https://my.company.com/somepage.wma");
        assertFalse(crawler.shouldVisit(url));
        url.setURL("https://my.company.com/somepage.zip");
        assertFalse(crawler.shouldVisit(url));
        url.setURL("https://my.company.com/somepage.rar");
        assertFalse(crawler.shouldVisit(url));
        url.setURL("https://my.company.com/somepage.gz");
        assertFalse(crawler.shouldVisit(url));

        url.setURL("https://my.company.com/img/somepic.gif?v=2");
        assertFalse(crawler.shouldVisit(url));
        url.setURL("https://my.company.com/css/somecss.css?v=2");
        assertFalse(crawler.shouldVisit(url));
    }

    private void doTestIncorrectMediaUrlsAreIgnored() {
        WebURL url = new WebURL();

        url.setURL("https://my.company.com/some.jpg/");
        assertFalse(crawler.shouldVisit(url));
        url.setURL("https://my.company.com/some.jpeg/");
        assertFalse(crawler.shouldVisit(url));
        url.setURL("https://my.company.com/some.gif/");
        assertFalse(crawler.shouldVisit(url));
        url.setURL("https://my.company.com/path1/path2/some.jpg/");
        assertFalse(crawler.shouldVisit(url));
        url.setURL("https://my.company.com/path1\\path2\\some.jpg/");
        assertFalse(crawler.shouldVisit(url));
    }

    public void testBadUrlsAreIgnored() {
        WebURL badUrl = new WebURL();
        badUrl.setURL("https://my.company.com/some-broken-url");
        crawler.handlePageStatusCode(badUrl, 400, "");
        crawler.visit(new Page(badUrl));
        assertEquals(0, HtmlOnlyCrawler.getGlobalPageCount());

        WebURL goodUrl = new WebURL();
        goodUrl.setURL("https://my.company.com/some-good-url");
        crawler.visit(new Page(goodUrl));
        assertEquals(1, HtmlOnlyCrawler.getGlobalPageCount());
    }
}
