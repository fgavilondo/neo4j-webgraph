package org.neo4japps.webgraph.util;

import java.util.Collections;
import java.util.List;

public class ListChunker<T> {

    private final int chunkSize;
    private final List<T> list;

    int fromIndex;
    int toIndex;

    public ListChunker(List<T> list, int chunkSize) {
        if (chunkSize < 1) {
            throw new IllegalArgumentException("chunkSize must be greater than one");
        }
        this.chunkSize = chunkSize;
        this.list = list == null ? Collections.emptyList() : list;

        initIndices();
    }

    protected void initIndices() {
        fromIndex = 0;
        toIndex = Math.min(list.size(), chunkSize);
    }

    public boolean hasMore() {
        return fromIndex < list.size();
    }

    public List<T> getNextChunk() {
        List<T> chunk = list.subList(fromIndex, toIndex);
        advanceIndices();
        return chunk;
    }

    private void advanceIndices() {
        fromIndex = toIndex;
        toIndex = Math.min(list.size(), (toIndex + chunkSize));
    }
}
