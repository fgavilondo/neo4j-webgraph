package org.neo4japps.webgraph.importer;

import org.neo4j.graphdb.RelationshipType;

enum RelTypes implements RelationshipType {
    ROOT_PAGE_REFERENCE, LINKS_TO
}
