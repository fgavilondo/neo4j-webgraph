package org.neo4japps.webgraph.customhandlers;

import org.apache.log4j.Logger;
import org.neo4japps.webgraph.util.SimpleHttpClient;

/**
 * This base class is thread-safe.
 */
public abstract class SocialMediaClientBase implements SocialMediaClient {
    protected final Logger logger = Logger.getLogger(this.getClass());

    protected final SimpleHttpClient httpClient;

    private final Object mutex = new Object();

    private volatile int politenessDelay = 0;
    private volatile long lastFetchTime = 0;

    public SocialMediaClientBase(SimpleHttpClient httpClient) {
        this.httpClient = httpClient;
    }

    @Override
    public void setPolitenessDelay(int value) {
        assert value >= 0;
        this.politenessDelay = value;
    }

    @Override
    public final int getSocialCount(String pageUrl) throws Exception {
        if (pageUrl == null)
            return SocialJsonResponse.UNINITALIZED;

        final String queryUrl = getSocialApiUrl() + cleanUp(pageUrl);

        int retryCounter = 0;

        while (true) {
            try {
                waitPolitely();
                String jsonString = httpClient.httpGet(queryUrl);
                SocialJsonResponse jsonObject = deserialize(jsonString, queryUrl);
                return jsonObject.getCount();
            } catch (ServiceUnavailableException e) {
                if (retryCounter == 10) {
                    logger.warn(e.getMessage() + ". Giving up.");
                    throw e;
                }
                retryCounter++;
                logger.warn(e.getMessage() + ". Retrying... " + retryCounter);
                sleep(1000);
            }
        }
    }

    private void waitPolitely() {
        if (politenessDelay == 0) {
            return;
        }

        synchronized (mutex) {
            final long millisSinceLastFetch = System.currentTimeMillis() - lastFetchTime;
            if (millisSinceLastFetch < politenessDelay) {
                sleep(politenessDelay - millisSinceLastFetch);
            }
            lastFetchTime = System.currentTimeMillis();
        }
    }

    private void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ignore) {
            Thread.currentThread().interrupt();
        }
    }

    protected abstract String getSocialApiUrl();

    protected abstract String cleanUp(String url);

    protected abstract SocialJsonResponse deserialize(String jsonString, String url) throws ServiceUnavailableException;

}
