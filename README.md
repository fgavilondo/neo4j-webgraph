# Project Info 

Neo4j-Webgraph is a Java application to crawl the configured websites and import their pages and links as nodes and relationships into a Neo4j graph database.

You configure the application by placing a config.properties file at the root of the classpath (a template config file is provided with the source code in the src/main/resources directory)

The application takes some command line parameters. Pass the -h argument to see all possible command line options, e.g.:

  java -cp target/neo4j-webgraph-1.8.3.0-jar-with-dependencies.jar org.neo4japps.webgraph.importer.Main -h

Note that, during the import process, you can annotate your Neo4j graph nodes with additional properties by providing custom event handlers that get invoked when a node gets created.

Sample Twitter and Facebook event handlers are provided with the source code. They determine how often a page has been "liked" (Facebook) or twitted about using these services' remote  APIs. See the source code for details.

# Project set-up and build

1. Download and install the Java JDK 1.7 or newer
2. Download and install Maven 3.1 or newer
3. Configure the import options in src/main/resources/config.properties
4. Build the project using build.[cmd|sh]

    If the build is successful a jar file called neo4j-webgraph-<VERSION>-jar-with-dependencies.jar is generated in the target directory.
   
5. Optional: Generate Eclipse project files (only needed for development in Eclipse): run updateEclipseProject.[cmd|sh]
6. Optional: Configure your local Maven repo in Eclipse (only needed for development in Eclipse)

    mvn -Declipse.workspace=<path-to-eclipse-workspace> eclipse:add-maven-repo

7. Run the importer using import.[cmd|sh]

    The importer accepts a number of command line options. To see them run 'import -h' or 'importer -?'.
    If you are not sure just accept the defaults.

8. Use the graph

    The import process creates a Neo4J graph database in the graph.db subdirectory.

    To use the imported graph you must download and install Neo4J from the [Neo4J website](http://www.neo4j.org/).
    It is recommended you use the same Neo4J version that was used to build the graph. For exmaple, if the neo4j-webgraph version is 1.8.3.0 then you need Neo4J 1.8.3.
    Note that Neo4J can be installed on a different machine to the one used for building/running the import.
    To run Neo4J all you need is a JRE. All other tools mentioned above are needed only for building neo4j-webgraph. 
   
    The easiest way is to copy the entire graph.db directory into the 'data' subdirectory of your Neo4J installation, for example neo4j-community-1.8.3\data\graph.db, and then (re)start Neo4J.

    Once Neo4J has been started you can point your browser at http://HOSTNAME:7474/webadmin/ to you browse and query the graph. See the Neo4J documentation for details.
   
    By default, for security reasons, the webadmin interface is only accessible from the machine running Neo4J. So if your Neo4J server is running on a remote machine (i.e not localhost) you need to un-comment the line:
     #org.neo4j.server.webserver.address=0.0.0.0
    in the conf/neo4j-server.properties file on the server and restart Neo4J.

# Exploring the graph using the Neo4J Cypher query language

Once you have imported the website(s) into a Neo4J graph you can browse the graph using Cypher queries. Cypher is the Neo4J graph query language (SQL-like).

Note: there are other ways of browsing the graph. Cypher is only one of them. See Neo4J documentation for details.

Using the webadmin app you can use the Neo4J remote shell page to run your Cypher queries, e.g. http://localhost:7474/webadmin/#/console/

You can also use the neo4j-shell utility in the bin directory.
 
Here are some sample queries. Note that in many cases it is convenient to find the start node(s) for the query via index lookup, like this:

    start p1=node:index-name(key="value"), p2=node:index-name(key="value")
       where ...

Queries:

* Find a node with a specific id, eg 152. This node may or may not be a web page node (in which case the 'url' property will be undefined).

    start p=node(3) return p.url

* Selected a group of nodes by id

    start p=node(152,153,154)
      return p.url

* Find a page node with a specific URL

    start p=node:pages(url="http://mydomain.com/")
      return p.url, p.incomingLinks, p.outgoingLinks, p.facebookTotalCount, p.twitterCount

* Find all pages that link to a particular page

    start p=node:pages(url="http://subdomain.mydomain.com/")
      match linkingpage-[:LINKS_TO]->p
      return count(linkingpage)
  
* Find all page nodes with at least 1000 incoming links

    start p=node:pages("url:*")
      where p.incomingLinks >= 1000
      return p.incomingLinks, p.url
      order by p.incomingLinks desc

    // attempt to do the same without using p.incomingLinks property
    // unfortunately "group by/having" does not work (yet) in Cypher :(
    start p=node:pages("url:*")
      match linkingpage-[:LINKS_TO]->p
      group by p.url
      having count(linkingpage) >= 1000
      return count (linkingpage), p.url
  
    // an alternative is to return the x top pages with the most number of links, e.g.
    start p=node:pages("url:*")
      match linkingpage-[:LINKS_TO]->p
      return count (linkingpage) as nrOflinks, p.url
      order by nrOflinks desc
      limit 50
  
* Count all the home pages

    start p=node:pages(type="home")
      return count (p)
  
* Find all the home pages, sorted by number of incoming links

    start p=node:pages(type="home")
      return p.url, p.incomingLinks, p.outgoingLinks 
      order by p.incomingLinks desc

* Find home page nodes with at least 50 incoming links

    start p=node:pages(type="home")
      where p.incomingLinks >= 50
      return p.url, p.incomingLinks
      order by p.incomingLinks desc
  
* Find number of pages in the 'somedomain' domain

    start p=node:pages(domain="somedomain") 
      return count (p)

* Find pages from the 'somedomain' domain with at least 50 incoming links

    start p=node:pages(domain="somedomain") 
      where p.incomingLinks >= 50
      return p.url, p.incomingLinks
      order by p.incomingLinks desc

* Find pages outside the 'somedomain' domain with at least 100 incoming links, being linked to from pages from the somedomain domain

    start somedomainpage=node:pages(domain="somedomain") 
      match p-[:LINKS_TO]->linkedpage
      where linkedpage.incomingLinks >= 100 and linkedpage.domain <> 'somedomain'
      return p.url, linkedpage.url, linkedpage.incomingLinks
      order by linkedpage.incomingLinks desc
  
* Find all page nodes containing a certain text (i.e. where content matches a certain regexp)

    start p=node:pages("url:*") 
      where p.content =~ /Page not yet fetched.*/
      return count(p)

* Find page nodes with un-initialised Facebook/Twitter properties

    start p=node:pages("url:*")
      where not has(p.facebookTotalCount)
      return p.url

    start p=node:pages("url:*")
      where not has(p.twitterCount)
      return p.url
