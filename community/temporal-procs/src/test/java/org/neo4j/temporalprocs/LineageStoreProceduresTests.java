/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.temporalprocs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Session;
import org.neo4j.harness.Neo4j;
import org.neo4j.harness.Neo4jBuilders;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.temporalgraph.entities.Property;
import org.neo4j.temporalgraph.entities.PropertyType;
import org.neo4j.temporalgraph.lineageindex.EntityLineageTracker;
import org.neo4j.temporalprocs.result.ResultNode;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectorySupportExtension;
import org.neo4j.test.utils.TestDirectory;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith({TestDirectorySupportExtension.class})
class LineageStoreProceduresTests {
    @Inject
    private TestDirectory directory;

    private static final String NODE_STORE_INDEX = "NODE_STORE_INDEX";
    private static final String REL_STORE_INDEX = "REL_STORE_INDEX";

    private Driver driver;
    private Neo4j embeddedDatabaseServer;
    private EntityLineageTracker tracker;

    @BeforeEach
    void init() {
        this.embeddedDatabaseServer = Neo4jBuilders.newInProcessBuilder()
                .withDisabledServer()
                .withProcedure(LineageStoreProcedures.class)
                .build();

        this.driver = GraphDatabase.driver(embeddedDatabaseServer.boltURI());
    }

    @AfterEach
    void teardown() throws IOException {
        if (embeddedDatabaseServer != null) {
            tracker.shutdown();
            tracker = null;
            embeddedDatabaseServer.databaseManagementService().shutdown();
            embeddedDatabaseServer = null;
        }
        this.driver.close();
        this.driver = null;
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1})
    void shouldReturnTheValidNode(int type) {
        // First, register the lineagestore listener
        registerTracker(type);

        // In a try-block, to make sure we close the session after the test
        try (Session session = driver.session()) {

            session.run("CREATE (n:Node {row_number: 0}) RETURN n;").single();
            var timestamp1 = tracker.getLastCommittedTime();
            session.run("MATCH (n:Node {row_number:0}) SET n.age = 42 RETURN n;")
                    .single();
            var timestamp2 = tracker.getLastCommittedTime();

            checkNodeExists(
                    session, 0, timestamp1, TestUtils.createResultNode(timestamp1, 0, "Node", new ArrayList<>() {
                        {
                            add(new Property(PropertyType.LONG, "row_number", 0L));
                        }
                    }));

            checkNodeExists(
                    session, 0, timestamp2, TestUtils.createResultNode(timestamp2, 0, "Node", new ArrayList<>() {
                        {
                            add(new Property(PropertyType.LONG, "row_number", 0L));
                            add(new Property(PropertyType.LONG, "age", 42L));
                        }
                    }));
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1})
    void shouldReturnTheValidNodesAfterDeletion(int type) {
        // First, register the lineagestore listener
        registerTracker(type);

        // In a try-block, to make sure we close the session after the test
        try (Session session = driver.session()) {

            session.run("CREATE (n:Node {row_number: 0}) RETURN n;").consume();
            session.run("CREATE (n:Node {row_number: 1}) RETURN n;").consume();
            session.run("MATCH (n) DELETE n;").consume();
            Thread.sleep(1000); // wait until the nodeIds are recycled
            session.run("CREATE (n:User {id: 5}) RETURN n;").consume();
            var timestamp = tracker.getLastCommittedTime();

            var expectedNode = TestUtils.createResultNode(timestamp, 0, "User", new ArrayList<>() {
                {
                    add(new Property(PropertyType.LONG, "id", 5L));
                }
            });

            // Check for the single node
            checkNodeExists(session, 0, timestamp, expectedNode);

            // Get all valid nodes
            var record = session.run(String.format("CALL lineage.getTemporalAllNodes(%d)", timestamp))
                    .single();
            assertEquals(record.get(0).asLong(), expectedNode.timestamp);
            assertEquals(record.get(1).asLong(), expectedNode.nodeId);
            assertEquals(record.get(2).asList(), expectedNode.labels);
            assertEquals(record.get(3).asMap(), expectedNode.properties);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1})
    void shouldReturnValidNeighbours(int type) {
        // First, register the lineagestore listener
        registerTracker(type);

        // In a try-block, to make sure we close the session after the test
        try (Session session = driver.session()) {

            // Create nodes 0-3
            session.run("CREATE (n:Node {row_number: 0}) RETURN n;").single();
            session.run("CREATE (n:Node {row_number: 1}) RETURN n;").single();
            session.run("CREATE (n:Node {row_number: 2}) RETURN n;").single();
            session.run("CREATE (n:Node {row_number: 3}) RETURN n;").single();
            var timestamp1 = tracker.getLastCommittedTime();

            // Create relationships 0->1, 0->2, 3->0
            session.run("MATCH (a:Node), (b:Node) " + "WHERE a.row_number = 0 AND b.row_number = 1 "
                            + "CREATE (a)-[r:KNOWS]->(b) "
                            + "RETURN r;")
                    .single();
            session.run("MATCH (a:Node), (b:Node) " + "WHERE a.row_number = 0 AND b.row_number = 2 "
                            + "CREATE (a)-[r:KNOWS]->(b) "
                            + "RETURN r;")
                    .single();
            session.run("MATCH (a:Node), (b:Node) " + "WHERE a.row_number = 3 AND b.row_number = 0 "
                            + "CREATE (a)-[r:KNOWS]->(b) "
                            + "RETURN r;")
                    .single();
            var timestamp2 = tracker.getLastCommittedTime();

            // Get the results before adding any edges
            var recordsBeforeEdges =
                    session.run(String.format("CALL lineage.expandNode(%d, %d, %d, %d) YIELD *", 0, 1, 1, timestamp1));
            assertFalse(recordsBeforeEdges.hasNext());

            // Get the results after adding the edges
            var recordsWithEdges = session.run(
                            String.format("CALL lineage.expandNode(%d, %d, %d, %d) YIELD *", 0, 1, 1, timestamp2))
                    .list();
            recordsWithEdges.sort(
                    Comparator.comparingLong((Record r) -> r.get(0).asLong()));

            // The first result is node 1
            assertEquals(0L, recordsWithEdges.get(0).get(1).asLong());
            assertEquals(1L, recordsWithEdges.get(0).get(2).asLong());
            assertEquals(1L, recordsWithEdges.get(0).get(4).asMap().get("row_number"));
            // The second result is node 2
            assertEquals(0L, recordsWithEdges.get(1).get(1).asLong());
            assertEquals(2L, recordsWithEdges.get(1).get(2).asLong());
            assertEquals(2L, recordsWithEdges.get(1).get(4).asMap().get("row_number"));
        }
    }

    /*@ParameterizedTest
    @ValueSource(ints = {0, 1})
    void shouldCreateValidProjection(int type) {
        // First, register the lineagestore listener
        registerTracker(type);

        // In a try-block, to make sure we close the session after the test
        try (Session session = driver.session()) {

            // Create nodes 0-3
            session.run("CREATE (n1:Node {row_number: 0}), " + "(n2:Node {row_number: 1}), "
                            + "(n3:Node {row_number: 2}), "
                            + "(n4:Node {row_number: 3}) "
                            + "RETURN n1, n2, n3, n4;")
                    .single();
            var timestamp1 = tracker.getLastCommittedTime();

            // Create relationships 0->1, 0->2, 3->0
            session.run("MATCH (a:Node), (b:Node) " + "WHERE a.row_number = 0 AND b.row_number = 1 "
                            + "CREATE (a)-[r:KNOWS {duration: 2}]->(b) "
                            + "RETURN r;")
                    .single();
            session.run("MATCH (a:Node), (b:Node) " + "WHERE a.row_number = 0 AND b.row_number = 2 "
                            + "CREATE (a)-[r:KNOWS {duration: 4}]->(b) "
                            + "RETURN r;")
                    .single();
            session.run("MATCH (a:Node), (b:Node) " + "WHERE a.row_number = 3 AND b.row_number = 0 "
                            + "CREATE (a)-[r:KNOWS {duration: 13}]->(b) "
                            + "RETURN r;")
                    .single();
            var timestamp2 = tracker.getLastCommittedTime();

            // Expected values before adding any edges
            String expectedNameBefore = "neo4jt" + timestamp1;
            long expectedNodeCountBefore = 4;
            long expectedRelationshipCountBefore = 0;

            // Get the results before adding the edges
            var recordsBeforeEdges = session.run(
                            String.format("CALL lineage.temporalProjection(%d) YIELD *", timestamp1))
                    .next();
            assertEquals(recordsBeforeEdges.get(0).asString(), expectedNameBefore);
            assertEquals(recordsBeforeEdges.get(1).asLong(), expectedNodeCountBefore);
            assertEquals(recordsBeforeEdges.get(2).asLong(), expectedRelationshipCountBefore);

            // Expected values after adding edges
            String expectedNameAfter = "neo4jt" + timestamp2;
            long expectedNodeCountAfter = 4;
            long expectedRelationshipCountAfter = 3;

            // Get the results after adding edges
            var recordsAfterEdges = session.run(
                            String.format("CALL lineage.temporalProjection(%d) YIELD *", timestamp2))
                    .next();
            assertEquals(recordsAfterEdges.get(0).asString(), expectedNameAfter);
            assertEquals(recordsAfterEdges.get(1).asLong(), expectedNodeCountAfter);
            assertEquals(recordsAfterEdges.get(2).asLong(), expectedRelationshipCountAfter);

            // Check that the projection is properly installed
            var projection = session.run("CALL gds.graph.list() " + "YIELD graphName, nodeCount, relationshipCount "
                            + "RETURN graphName, nodeCount, relationshipCount "
                            + "ORDER BY graphName ASC")
                    .list();
            assertEquals(projection.get(0).get(0).asString(), expectedNameBefore);
            assertEquals(projection.get(0).get(1).asLong(), expectedNodeCountBefore);
            assertEquals(projection.get(0).get(2).asLong(), expectedRelationshipCountBefore);

            assertEquals(projection.get(1).get(0).asString(), expectedNameAfter);
            assertEquals(projection.get(1).get(1).asLong(), expectedNodeCountAfter);
            assertEquals(projection.get(1).get(2).asLong(), expectedRelationshipCountAfter);

            */
    /*var nodes = session.run(String.format(
                    "CALL gds.graph.nodeProperties.stream('%s', ['row_number']) "
                            + "YIELD nodeId, nodeProperty, propertyValue "
                            + "RETURN nodeId, nodeProperty, propertyValue",
                    expectedNameAfter))
            .list();

    System.out.println(nodes);

    var rels = session.run(String.format(
                    "CALL gds.graph.relationshipProperties.stream('%s', ['duration'], ['KNOWS']) "
                            + "YIELD sourceNodeId, targetNodeId, relationshipType, relationshipProperty, propertyValue "
                            + "RETURN sourceNodeId, targetNodeId, relationshipType, relationshipProperty, propertyValue",
                    expectedNameAfter))
            .list();

    System.out.println(rels);

    session.run(String.format("CALL gds.alpha.create.cypherdb('gdsdb', '%s')", expectedNameAfter));
    var srels = session.run("USE gdsdb match ()-[r:KNOWS]->() return r, r.duration ")
            .list();
    System.out.println(srels);*/
    /*
        }
    }*/

    @ParameterizedTest
    @ValueSource(ints = {0, 1})
    void shouldReturnTheValidNodeHistory(int type) {
        // First, register the lineagestore listener
        registerTracker(type);

        // In a try-block, to make sure we close the session after the test
        try (Session session = driver.session()) {

            session.run("CREATE (n:Node {row_number: 1}) RETURN n;").single();
            var timestamp1 = tracker.getLastCommittedTime();
            session.run("MATCH (n:Node {row_number:1}) SET n.age = 42 RETURN n;")
                    .single();
            var timestamp2 = tracker.getLastCommittedTime();
            session.run("MATCH (n:Node {row_number:1}) DELETE n;").consume();
            var timestamp3 = tracker.getLastCommittedTime();

            var nodeHistory1 = session.run(
                            String.format("CALL lineage.getTemporalNodeHistory(%d, %d, %d) YIELD *", 0, 0, timestamp1))
                    .list();
            assertEquals(1, nodeHistory1.size());
            assertEquals(timestamp1, nodeHistory1.get(0).get(0).asLong());
            assertEquals(Long.MAX_VALUE, nodeHistory1.get(0).get(1).asLong());
            assertEquals(0L, nodeHistory1.get(0).get(2).asLong());
            assertEquals(1L, nodeHistory1.get(0).get(4).asMap().get("row_number"));

            var nodeHistory2 = session.run(String.format(
                            "CALL lineage.getTemporalNodeHistory(%d, %d, %d) YIELD *", 0, timestamp1, timestamp2))
                    .list();
            assertEquals(2, nodeHistory2.size());
            assertEquals(timestamp1, nodeHistory2.get(0).get(0).asLong());
            assertEquals(timestamp2, nodeHistory2.get(0).get(1).asLong());

            assertEquals(timestamp2, nodeHistory2.get(1).get(0).asLong());
            assertEquals(Long.MAX_VALUE, nodeHistory2.get(1).get(1).asLong());
            assertEquals(42L, nodeHistory2.get(1).get(4).asMap().get("age"));

            var nodeHistory3 = session.run(String.format(
                            "CALL lineage.getTemporalNodeHistory(%d, %d, %d) YIELD *", 0, timestamp1, timestamp3))
                    .list();
            assertEquals(2, nodeHistory3.size());
            assertEquals(timestamp1, nodeHistory3.get(0).get(0).asLong());
            assertEquals(timestamp2, nodeHistory3.get(0).get(1).asLong());

            assertEquals(timestamp2, nodeHistory3.get(1).get(0).asLong());
            assertEquals(timestamp3, nodeHistory3.get(1).get(1).asLong());
        }
    }

    public static void checkNodeExists(Session session, long nodeId, long timestamp, ResultNode expectedNode) {
        var record = session.run(String.format("CALL lineage.getTemporalNode(%d, %d) YIELD *", nodeId, timestamp))
                .single();

        assertEquals(record.get(0).asLong(), expectedNode.timestamp);
        assertEquals(record.get(1).asLong(), expectedNode.nodeId);
        assertEquals(record.get(2).asList(), expectedNode.labels);
        assertEquals(record.get(3).asMap(), expectedNode.properties);
    }

    private void registerTracker(int type) {
        var dbms = embeddedDatabaseServer.databaseManagementService();
        if (type == 0) {
            tracker = new EntityLineageTracker();
        } else if (type == 1) {
            var pageCache = (PageCache) dbms.database(DEFAULT_DATABASE_NAME).getPageCache();
            var fs =
                    (FileSystemAbstraction) dbms.database(DEFAULT_DATABASE_NAME).getFileSystem();
            var nodeIndexPath = directory.homePath().resolve(NODE_STORE_INDEX);
            var relIndexPath = directory.homePath().resolve(REL_STORE_INDEX);

            tracker = new EntityLineageTracker(pageCache, fs, nodeIndexPath, relIndexPath);
        } else {
            throw new IllegalArgumentException(String.format("Type %d is not supported", type));
        }

        dbms.registerTransactionEventListener(DEFAULT_DATABASE_NAME, tracker);
    }
}
