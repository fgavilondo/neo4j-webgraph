#!/bin/sh

# Pass -h argument to see all possible command line options
#  import.sh -h

# 1GB heap space can accommodate roughly 35000 nodes/pages (which are kept in an in-memory index for fast access)
# For larger graphs you must increase the -Xmx value accordingly.

java -Xms512m -Xmx1024m -cp target/webgraph-1.0-jar-with-dependencies.jar org.neo4japps.webgraph.importer.Main $*