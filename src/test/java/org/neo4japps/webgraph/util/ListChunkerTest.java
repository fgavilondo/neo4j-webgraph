package org.neo4japps.webgraph.util;

import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;

public class ListChunkerTest extends TestCase {

    private static final int CHUNK_SIZE = 5;

    private ListChunker<Integer> chunker;
    private List<Integer> chunk;

    public void testChunkSizeHasToBeGreaterThanZero() {
        try {
            chunker = new ListChunker<Integer>(createTestList(0), -1);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
        }

        try {
            chunker = new ListChunker<Integer>(createTestList(0), 0);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
        }
    }

    public void testThatThereAreNoChunksForNullLists() {
        chunker = new ListChunker<Integer>(null, CHUNK_SIZE);
        assertFalse(chunker.hasMore());
        chunk = chunker.getNextChunk();
        assertEquals(0, chunk.size());
        assertFalse(chunker.hasMore());
        assertEquals(0, chunker.getNextChunk().size());
    }

    public void testThatThereAreNoChunksForEmptyLists() {
        chunker = new ListChunker<Integer>(createTestList(0), CHUNK_SIZE);
        assertFalse(chunker.hasMore());
        chunk = chunker.getNextChunk();
        assertEquals(0, chunk.size());
        assertFalse(chunker.hasMore());
        assertEquals(0, chunker.getNextChunk().size());
    }

    public void testChunkingForChunkSizOfOne() {
        List<Integer> testList = createTestList(3);
        chunker = new ListChunker<Integer>(testList, 1);

        assertTrue(chunker.hasMore());
        chunk = chunker.getNextChunk();
        assertEquals(1, chunk.size());

        assertTrue(chunker.hasMore());
        chunk = chunker.getNextChunk();
        assertEquals(1, chunk.size());

        assertTrue(chunker.hasMore());
        chunk = chunker.getNextChunk();
        assertEquals(1, chunk.size());

        assertFalse(chunker.hasMore());
        assertEquals(0, chunker.getNextChunk().size());
    }

    public void testChunkingForChunkSizeGreaterThanOne() {
        List<Integer> testList = createTestList(1);
        chunker = new ListChunker<Integer>(testList, CHUNK_SIZE);
        assertTrue(chunker.hasMore());
        chunk = chunker.getNextChunk();
        assertEquals(1, chunk.size());
        assertFalse(chunker.hasMore());
        assertEquals(0, chunker.getNextChunk().size());

        testList = createTestList(9);
        chunker = new ListChunker<Integer>(testList, CHUNK_SIZE);
        assertTrue(chunker.hasMore());
        chunk = chunker.getNextChunk();
        assertEquals(CHUNK_SIZE, chunk.size());
        assertTrue(chunker.hasMore());
        chunk = chunker.getNextChunk();
        assertEquals(4, chunk.size());
        assertFalse(chunker.hasMore());
        assertEquals(0, chunker.getNextChunk().size());

        testList = createTestList(10);
        chunker = new ListChunker<Integer>(testList, CHUNK_SIZE);
        assertTrue(chunker.hasMore());
        chunk = chunker.getNextChunk();
        assertEquals(CHUNK_SIZE, chunk.size());
        assertTrue(chunker.hasMore());
        chunk = chunker.getNextChunk();
        assertEquals(CHUNK_SIZE, chunk.size());
        assertFalse(chunker.hasMore());
        assertEquals(0, chunker.getNextChunk().size());

        testList = createTestList(11);
        chunker = new ListChunker<Integer>(testList, CHUNK_SIZE);
        assertTrue(chunker.hasMore());
        chunk = chunker.getNextChunk();
        assertEquals(CHUNK_SIZE, chunk.size());
        assertTrue(chunker.hasMore());
        chunk = chunker.getNextChunk();
        assertEquals(CHUNK_SIZE, chunk.size());
        assertTrue(chunker.hasMore());
        chunk = chunker.getNextChunk();
        assertEquals(1, chunk.size());
        assertFalse(chunker.hasMore());
        assertEquals(0, chunker.getNextChunk().size());
    }

    private List<Integer> createTestList(int size) {
        List<Integer> list = new ArrayList<Integer>(size);
        for (int i = 0; i < size; i++) {
            list.add(i);
        }
        return list;
    }
}
