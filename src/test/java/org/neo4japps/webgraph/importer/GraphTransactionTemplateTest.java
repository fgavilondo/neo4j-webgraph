package org.neo4japps.webgraph.importer;

import java.util.concurrent.Callable;

import junit.framework.TestCase;

import org.neo4j.kernel.DeadlockDetectedException;
import org.neo4j.test.TestGraphDatabaseFactory;

public class GraphTransactionTemplateTest extends TestCase {
    static class CallableThatAlwaysDeadlocks implements Callable<Object> {
        int callCounter = 0;

        @Override
        public Object call() throws Exception {
            callCounter++;
            throw new DeadlockDetectedException("Fake deadlock");
        }

        @Override
        public String toString() {
            return "Fake callable that always deadlocks";
        }
    }

    static class CallableThatThrowsGenericException implements Callable<Object> {
        int callCounter = 0;

        @Override
        public Object call() throws Exception {
            callCounter++;
            throw new Exception("Fake exception");
        }

        @Override
        public String toString() {
            return "Fake callable that throws exception";
        }
    }

    private GraphImporter graphImporter;

    @Override
    protected void setUp() {
        graphImporter = new TransactionalGraphImporter(new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder()
                .newGraphDatabase(), "http://homepage.com/", System.currentTimeMillis(), 100, 500);
    }

    @Override
    protected void tearDown() {
        graphImporter.shutdown();
    }

    public void testThatItCorrectlyRetriesOnDeadlock() throws Exception {
        GraphTransactionTemplate template = new GraphTransactionTemplate(this);

        CallableThatAlwaysDeadlocks task = new CallableThatAlwaysDeadlocks();
        template.execute(task, graphImporter, 0, 10);
        assertEquals(1, task.callCounter);

        task = new CallableThatAlwaysDeadlocks();
        template.execute(task, graphImporter, 5, 10);
        assertEquals(6, task.callCounter);
    }

    public void testThatItDoesNotRetryOnGenericException() throws Exception {
        GraphTransactionTemplate template = new GraphTransactionTemplate(this);

        CallableThatThrowsGenericException task = new CallableThatThrowsGenericException();
        try {
            template.execute(task, graphImporter, 0, 10);
            fail("Expected exception");
        } catch (Exception expected) {
            assertEquals(1, task.callCounter);
            assertEquals("Fake exception", expected.getMessage());
        }

        task = new CallableThatThrowsGenericException();
        try {
            template.execute(task, graphImporter, 5, 10);
            fail("Expected exception");
        } catch (Exception expected) {
            assertEquals(1, task.callCounter);
            assertEquals("Fake exception", expected.getMessage());
        }
    }
}
