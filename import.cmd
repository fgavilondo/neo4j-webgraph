@echo off

rem Pass -h argument to see all possible command line options
rem   import.cmd -h

rem 1GB heap space can accommodate roughly 35000 nodes/pages (which are kept in an in-memory index for fast access)
rem For larger graphs you must increase the -Xmx value.

java -Xms512m -Xmx1024m -cp target/neo4j-webgraph-1.8.3.0-jar-with-dependencies.jar org.neo4japps.webgraph.importer.Main %*