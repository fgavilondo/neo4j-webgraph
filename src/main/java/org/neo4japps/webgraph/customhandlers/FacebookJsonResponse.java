package org.neo4japps.webgraph.customhandlers;

public class FacebookJsonResponse extends SocialJsonResponse {
    public int share_count = UNINITALIZED;
    public int like_count = UNINITALIZED;
    public int comment_count = UNINITALIZED;
    public int total_count = UNINITALIZED;
    public int click_count = UNINITALIZED;

    @Override
    public String toString() {
        return "FacebookJsonResponse [share_count=" + share_count + ", like_count=" + like_count + ", comment_count="
                + comment_count + ", total_count=" + total_count + ", click_count=" + click_count + "]";
    }

    public boolean isInitialized() {
        return total_count != UNINITALIZED;
    }

    @Override
    public int getCount() {
        return total_count;
    }
}
