package org.neo4japps.webgraph.testutils;

import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.neo4j.graphdb.Node;

public class MockNodeFactory {
    final Map<Node, Map<String, Object>> propsByNode = new HashMap<Node, Map<String, Object>>();

    public void clear() {
        propsByNode.clear();
    }

    public Node newNode() {
        final Node mockNode = mock(Node.class);
        propsByNode.put(mockNode, new HashMap<String, Object>());

        // stub getters

        when(mockNode.getProperty(anyString())).thenAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) {
                Object[] args = invocation.getArguments();
                return propsByNode.get(mockNode).get(args[0]);
            }

        });
        when(mockNode.getProperty(anyString(), anyInt())).thenAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) {
                Object[] args = invocation.getArguments();
                return propsByNode.get(mockNode).get(args[0]) == null ? args[1] : propsByNode.get(mockNode)
                        .get(args[0]);
            }
        });

        // stub setters
        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) {
                Object[] args = invocation.getArguments();
                propsByNode.get(mockNode).put((String) args[0], args[1]);
                return null;
            }
        }).when(mockNode).setProperty(anyString(), anyObject());

        return mockNode;
    }
}
