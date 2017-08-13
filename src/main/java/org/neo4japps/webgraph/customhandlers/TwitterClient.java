package org.neo4japps.webgraph.customhandlers;

import org.neo4japps.webgraph.util.SimpleHttpClient;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;

/**
 * This class is thread-safe.
 */
public class TwitterClient extends SocialMediaClientBase {

    public TwitterClient(SimpleHttpClient httpClient) {
        super(httpClient);
    }

    @Override
    protected String getSocialApiUrl() {
        // this is a bit flaky as it is actually not a public API we get a lot
        // of
        // "Unable to access URL counting services" errors
        // see https://dev.twitter.com/discussions/9025
        return "http://urls.api.twitter.com/1/urls/count.json?url=";
    }

    @Override
    protected String cleanUp(String url) {
        // Twitter swallows pretty much anything
        return url;
    }

    @Override
    protected SocialJsonResponse deserialize(String jsonString, String url) throws ServiceUnavailableException {
        return doDeserialize(jsonString, url);
    }

    /**
     * Package private visibility for unit testing
     */
    TwitterJsonResponse doDeserialize(String jsonString, String url) throws ServiceUnavailableException {
        // Sample Twitter JSON:
        // {"count":3115,"url":"http://mydomain.com/"}

        if (jsonString != null && jsonString.contains("Unable to access URL counting services")) {
            // Sample error JSON:
            // {"errors":[{"code":48,"message":"Unable to access URL counting services"}]}
            throw new ServiceUnavailableException("Unable to access URL counting services for URL " + url);
        }

        TwitterJsonResponse res = null;

        try {
            if (jsonString == null || jsonString.trim().equals("")) {
                throw new JsonSyntaxException("Can't parse empty JSON string");
            }

            res = new Gson().fromJson(jsonString, TwitterJsonResponse.class);
        } catch (JsonSyntaxException e) {
            if (url != null) {
                // we use null URLs in unit test to avoid logging all the
                // exceptions
                logger.warn("Can't parse JSON (" + jsonString + ") returned by " + url, e);
            }

            res = new TwitterJsonResponse();
        }

        return res;
    }
}
