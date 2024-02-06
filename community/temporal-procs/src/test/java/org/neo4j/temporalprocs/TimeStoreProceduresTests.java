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
import static org.neo4j.temporalgraph.timeindex.SnapshotCreationPolicy.DEFAULT_POLICY;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Session;
import org.neo4j.gds.catalog.GraphListProc;
import org.neo4j.gds.catalog.GraphStreamNodePropertiesProc;
import org.neo4j.gds.catalog.GraphStreamRelationshipPropertiesProc;
import org.neo4j.gds.core.Settings;
import org.neo4j.gds.cypher.GraphCreateCypherDbProc;
import org.neo4j.harness.Neo4j;
import org.neo4j.harness.Neo4jBuilders;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.temporalgraph.entities.Property;
import org.neo4j.temporalgraph.entities.PropertyType;
import org.neo4j.temporalgraph.timeindex.timestore.TimeBasedTracker;
import org.neo4j.temporalprocs.result.ResultNode;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectorySupportExtension;
import org.neo4j.test.utils.TestDirectory;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith({TestDirectorySupportExtension.class})
class TimeStoreProceduresTests {
    @Inject
    private TestDirectory directory;

    private static final String TIMESTORE_LOG = "tslog";
    private static final String TIMESTORE_INDEX = "tsindex";

    private Driver driver;
    private Neo4j embeddedDatabaseServer;
    private TimeBasedTracker tracker;

    @BeforeEach
    void init() {
        this.embeddedDatabaseServer = Neo4jBuilders.newInProcessBuilder()
                .withDisabledServer()
                .withConfig(Settings.procedureUnrestricted(), List.of("gds.*"))
                .withConfig(GraphDatabaseSettings.procedure_allowlist, List.of("gds.*"))
                .withProcedure(TimeStoreProcedures.class)
                .withProcedure(GraphListProc.class)
                .withProcedure(GraphStreamNodePropertiesProc.class)
                .withProcedure(GraphStreamRelationshipPropertiesProc.class)
                .withProcedure(GraphCreateCypherDbProc.class)
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
    void shouldReturnTheValidNode(int type) throws IOException {
        // First, register the timestore listener
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
    void shouldReturnValidNeighbours(int type) throws IOException {
        // First, register the timestore listener
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
                    session.run(String.format("CALL time.expandNode(%d, %d, %d, %d) YIELD *", 0, 1, 1, timestamp1));
            assertFalse(recordsBeforeEdges.hasNext());

            // Get the results after adding the edges
            var recordsWithEdges = session.run(
                            String.format("CALL time.expandNode(%d, %d, %d, %d) YIELD *", 0, 1, 1, timestamp2))
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
    void shouldCreateValidProjection(int type) throws IOException {
        // First, register the timestore listener
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
            var recordsBeforeEdges = session.run(String.format("CALL time.temporalProjection(%d) YIELD *", timestamp1))
                    .next();
            assertEquals(recordsBeforeEdges.get(0).asString(), expectedNameBefore);
            assertEquals(recordsBeforeEdges.get(1).asLong(), expectedNodeCountBefore);
            assertEquals(recordsBeforeEdges.get(2).asLong(), expectedRelationshipCountBefore);

            // Expected values after adding edges
            String expectedNameAfter = "neo4jt" + timestamp2;
            long expectedNodeCountAfter = 4;
            long expectedRelationshipCountAfter = 3;

            // Get the results after adding edges
            var recordsAfterEdges = session.run(String.format("CALL time.temporalProjection(%d) YIELD *", timestamp2))
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
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1})
    void shouldCreateValidProjectionWithNodeDeletion(int type) throws IOException {
        // First, register the timestore listener
        registerTracker(type);

        // In a try-block, to make sure we close the session after the test
        try (Session session = driver.session()) {

            // Create nodes and relationships
            session.run("CREATE (n1:Node {row_number: 0}), " + "(n2:Node {row_number: 1}), "
                            + "(n3:Node {row_number: 2}), "
                            + "(n4:Node {row_number: 3}),"
                            + "(n1)-[:KNOWS {duration: 2}]->(n2), "
                            + "(n1)-[:KNOWS {duration: 4}]->(n3), "
                            + "(n4)-[:KNOWS {duration: 13}]->(n1);")
                    .consume();
            session.run("MATCH (n) WHERE id(n) = 1 DETACH DELETE n;").consume();
            var timestamp = tracker.getLastCommittedTime();

            // Expected values after adding edges
            String expectedNameAfter = "neo4jt" + timestamp;
            long expectedNodeCountAfter = 3;
            long expectedRelationshipCountAfter = 2;

            // Get the results after adding edges
            var recordsAfterEdges = session.run(String.format("CALL time.temporalProjection(%d) YIELD *", timestamp))
                    .next();
            assertEquals(recordsAfterEdges.get(0).asString(), expectedNameAfter);
            assertEquals(recordsAfterEdges.get(1).asLong(), expectedNodeCountAfter);
            assertEquals(recordsAfterEdges.get(2).asLong(), expectedRelationshipCountAfter);

            // Check that the projection is properly installed
            var projection = session.run("CALL gds.graph.list() " + "YIELD graphName, nodeCount, relationshipCount "
                            + "RETURN graphName, nodeCount, relationshipCount "
                            + "ORDER BY graphName ASC")
                    .list();
            assertEquals(projection.get(0).get(0).asString(), expectedNameAfter);
            assertEquals(projection.get(0).get(1).asLong(), expectedNodeCountAfter);
            assertEquals(projection.get(0).get(2).asLong(), expectedRelationshipCountAfter);
        }
    }*/

    @ParameterizedTest
    @ValueSource(ints = {1})
    void shouldReturnPageRank(int type) throws IOException {
        // First, register the timestore listener
        registerTracker(type);

        // In a try-block, to make sure we close the session after the test
        try (Session session = driver.session()) {

            // Create nodes 0-3
            session.run("CREATE (n1:Node {row_number: 0}), "
                            + "(n2:Node {row_number: 1}), "
                            + "(n3:Node {row_number: 2}), "
                            + "(n4:Node {row_number: 3}), "
                            + "(n5:Node {row_number: 4}), "
                            + "(n6:Node {row_number: 5}) "
                            + "RETURN n1, n2, n3, n4, n5, n6;")
                    .single();
            var timestamp1 = tracker.getLastCommittedTime();

            // Create relationships 0->1, 0->2, 3->0
            session.run("MATCH (a:Node), (b:Node) " + "WHERE a.row_number = 0 AND b.row_number = 1 "
                            + "CREATE (a)-[r:KNOWS {duration: 12}]->(b) "
                            + "RETURN r;")
                    .single();
            session.run("MATCH (a:Node), (b:Node) " + "WHERE a.row_number = 0 AND b.row_number = 2 "
                            + "CREATE (a)-[r:KNOWS {duration: 13}]->(b) "
                            + "RETURN r;")
                    .single();
            session.run("MATCH (a:Node), (b:Node) " + "WHERE a.row_number = 3 AND b.row_number = 0 "
                            + "CREATE (a)-[r:KNOWS {duration: 14}]->(b) "
                            + "RETURN r;")
                    .single();
            var timestamp2 = tracker.getLastCommittedTime();

            // Add more relationships 3->4, 3->5, 4->5
            session.run("MATCH (a:Node), (b:Node) " + "WHERE a.row_number = 3 AND b.row_number = 4 "
                            + "CREATE (a)-[r:KNOWS {duration: 15}]->(b) "
                            + "RETURN r;")
                    .single();
            session.run("MATCH (a:Node), (b:Node) " + "WHERE a.row_number = 3 AND b.row_number = 5 "
                            + "CREATE (a)-[r:KNOWS {duration: 16}]->(b) "
                            + "RETURN r;")
                    .single();
            session.run("MATCH (a:Node), (b:Node) " + "WHERE a.row_number = 4 AND b.row_number = 5 "
                            + "CREATE (a)-[r:KNOWS {duration: 17}]->(b) "
                            + "RETURN r;")
                    .single();
            var timestamp3 = tracker.getLastCommittedTime();

            // Get pagerank at timestamp 2
            var pagerank1 = session.run(String.format("CALL time.pageRank(%d) YIELD *", timestamp2))
                    .list();
            assertEquals(6, pagerank1.size());

            // Get pagerank at timestamp 3
            var pagerank2 = session.run(String.format("CALL time.pageRank(%d) YIELD *", timestamp3))
                    .list();
            ;
            assertEquals(6, pagerank2.size());

            // Get incremental pagerank between timestamp 2 and timestamp 3
            var pagerank3 = session.run(String.format(
                            "CALL time.dynamicPageRank(%d, %d, %d) YIELD *",
                            timestamp2, timestamp3, timestamp3 - timestamp2))
                    .list();
            assertEquals(12, pagerank3.size());
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {1})
    void shouldReturnAvg(int type) throws IOException {
        // First, register the timestore listener
        registerTracker(type);

        // In a try-block, to make sure we close the session after the test
        try (Session session = driver.session()) {

            // Create nodes 0-3
            session.run("CREATE (n1:Node {row_number: 0}), "
                            + "(n2:Node {row_number: 1}), "
                            + "(n3:Node {row_number: 2}), "
                            + "(n4:Node {row_number: 3}), "
                            + "(n5:Node {row_number: 4}), "
                            + "(n6:Node {row_number: 5}) "
                            + "RETURN n1, n2, n3, n4, n5, n6;")
                    .single();
            var timestamp1 = tracker.getLastCommittedTime();

            // Create relationships 0->1, 0->2, 3->0
            session.run("MATCH (a:Node), (b:Node) " + "WHERE a.row_number = 0 AND b.row_number = 1 "
                            + "CREATE (a)-[r:KNOWS {duration: 12}]->(b) "
                            + "RETURN r;")
                    .single();
            session.run("MATCH (a:Node), (b:Node) " + "WHERE a.row_number = 0 AND b.row_number = 2 "
                            + "CREATE (a)-[r:KNOWS {duration: 13}]->(b) "
                            + "RETURN r;")
                    .single();
            session.run("MATCH (a:Node), (b:Node) " + "WHERE a.row_number = 3 AND b.row_number = 0 "
                            + "CREATE (a)-[r:KNOWS {duration: 14}]->(b) "
                            + "RETURN r;")
                    .single();
            var timestamp2 = tracker.getLastCommittedTime();

            // Add more relationships 3->4, 3->5, 4->5
            session.run("MATCH (a:Node), (b:Node) " + "WHERE a.row_number = 3 AND b.row_number = 4 "
                            + "CREATE (a)-[r:KNOWS {duration: 15}]->(b) "
                            + "RETURN r;")
                    .single();
            session.run("MATCH (a:Node), (b:Node) " + "WHERE a.row_number = 3 AND b.row_number = 5 "
                            + "CREATE (a)-[r:KNOWS {duration: 16}]->(b) "
                            + "RETURN r;")
                    .single();
            session.run("MATCH (a:Node), (b:Node) " + "WHERE a.row_number = 4 AND b.row_number = 5 "
                            + "CREATE (a)-[r:KNOWS {duration: 17}]->(b) "
                            + "RETURN r;")
                    .single();
            var timestamp3 = tracker.getLastCommittedTime();

            // Get pagerank at timestamp 2
            var avg1 = session.run(String.format("CALL time.avg(%s, %d) YIELD *", "\"duration\"", timestamp2))
                    .list();
            assertEquals(13.0, avg1.get(0).get(1).asDouble());

            // Get pagerank at timestamp 3
            var avg2 = session.run(String.format("CALL time.avg(%s, %d) YIELD *", "\"duration\"", timestamp3))
                    .list();
            ;
            assertEquals(14.5, avg2.get(0).get(1).asDouble());

            // Get incremental pagerank between timestamp 2 and timestamp 3
            var results = session.run(String.format(
                            "CALL time.dynamicAvg(%s, %d, %d, %d) YIELD *",
                            "\"duration\"", timestamp2, timestamp3, timestamp3 - timestamp2))
                    .list();
            assertEquals(13.0, results.get(0).get(1).asDouble());
            assertEquals(14.5, results.get(1).get(1).asDouble());
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {1})
    void shouldReturnLabelRank(int type) throws IOException {
        // First, register the timestore listener
        registerTracker(type);

        // In a try-block, to make sure we close the session after the test
        try (Session session = driver.session()) {

            // Create nodes 0-3
            session.run("CREATE (n1:Node {row_number: 0}), "
                            + "(n2:Node {row_number: 1}), "
                            + "(n3:Node {row_number: 2}), "
                            + "(n4:Node {row_number: 3}), "
                            + "(n5:Node {row_number: 4}), "
                            + "(n6:Node {row_number: 5}) "
                            + "RETURN n1, n2, n3, n4, n5, n6;")
                    .single();
            var timestamp1 = tracker.getLastCommittedTime();

            // Create relationships 0->1, 0->2, 3->0
            session.run("MATCH (a:Node), (b:Node) " + "WHERE a.row_number = 0 AND b.row_number = 1 "
                            + "CREATE (a)-[r:KNOWS {duration: 12}]->(b) "
                            + "RETURN r;")
                    .single();
            session.run("MATCH (a:Node), (b:Node) " + "WHERE a.row_number = 0 AND b.row_number = 2 "
                            + "CREATE (a)-[r:KNOWS {duration: 13}]->(b) "
                            + "RETURN r;")
                    .single();
            session.run("MATCH (a:Node), (b:Node) " + "WHERE a.row_number = 3 AND b.row_number = 0 "
                            + "CREATE (a)-[r:KNOWS {duration: 14}]->(b) "
                            + "RETURN r;")
                    .single();
            var timestamp2 = tracker.getLastCommittedTime();

            // Add more relationships 3->4, 3->5, 4->5
            session.run("MATCH (a:Node), (b:Node) " + "WHERE a.row_number = 3 AND b.row_number = 4 "
                            + "CREATE (a)-[r:KNOWS {duration: 15}]->(b) "
                            + "RETURN r;")
                    .single();
            session.run("MATCH (a:Node), (b:Node) " + "WHERE a.row_number = 3 AND b.row_number = 5 "
                            + "CREATE (a)-[r:KNOWS {duration: 16}]->(b) "
                            + "RETURN r;")
                    .single();
            session.run("MATCH (a:Node), (b:Node) " + "WHERE a.row_number = 4 AND b.row_number = 5 "
                            + "CREATE (a)-[r:KNOWS {duration: 17}]->(b) "
                            + "RETURN r;")
                    .single();
            var timestamp3 = tracker.getLastCommittedTime();

            // Get pagerank at timestamp 2
            var labelrank1 = session.run(String.format("CALL time.labelRank(%d) YIELD *", timestamp2))
                    .list();
            assertEquals(6, labelrank1.size());

            // Get pagerank at timestamp 3
            var labelrank2 = session.run(String.format("CALL time.labelRank(%d) YIELD *", timestamp3))
                    .list();
            ;
            assertEquals(6, labelrank2.size());

            // Get incremental pagerank between timestamp 2 and timestamp 3
            var labelrank3 = session.run(String.format(
                            "CALL time.dynamicLabelRank(%d, %d, %d) YIELD *",
                            timestamp2, timestamp3, timestamp3 - timestamp2))
                    .list();
            assertEquals(12, labelrank3.size());
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {1})
    void shouldReturnLabelRank2(int type) throws IOException {
        // First, register the timestore listener
        registerTracker(type);

        // In a try-block, to make sure we close the session after the test
        try (Session session = driver.session()) {

            // Create a graph
            session.run(" CREATE " + "  (alice:User {posts: 4, seed_label: 52}),"
                            + "  (bridget:User {posts: 13, seed_label: 21}),"
                            + "  (charles:User {posts: 55, seed_label: 43}),"
                            + "  (doug:User {posts: 5, seed_label: 21}),"
                            + "  (mark:User {posts: 7, seed_label: 19}),"
                            + "  (michael:User {posts: 15, seed_label: 52}),"
                            + "  (alice)-[:FOLLOW {weight: 1}]->(bridget),"
                            + "  (alice)-[:FOLLOW {weight: 10}]->(charles),"
                            + "  (mark)-[:FOLLOW {weight: 1}]->(doug),"
                            + "  (bridget)-[:FOLLOW {weight: 1}]->(michael),"
                            + "  (doug)-[:FOLLOW {weight: 1}]->(mark),"
                            + "  (michael)-[:FOLLOW {weight: 1}]->(alice),"
                            + "  (alice)-[:FOLLOW {weight: 1}]->(michael),"
                            + "  (bridget)-[:FOLLOW {weight: 1}]->(alice),"
                            + "  (michael)-[:FOLLOW {weight: 1}]->(bridget),"
                            + "  (charles)-[:FOLLOW {weight: 1}]->(doug);")
                    .consume();
            var timestamp = tracker.getLastCommittedTime();

            // Get pagerank at timestamp 2
            var labelrank = session.run(String.format("CALL time.labelRank(%d) YIELD *", timestamp))
                    .list();
            assertEquals(6, labelrank.size());
            for (var l : labelrank) {
                assertEquals(1L, l.get(2).asLong());
            }
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {1})
    void shouldReturnBFS(int type) throws IOException {
        // First, register the timestore listener
        registerTracker(type);

        // In a try-block, to make sure we close the session after the test
        try (Session session = driver.session()) {

            // Create a graph
            session.run(" CREATE" + "       (nA:Node {name: 1}),"
                            + "       (nB:Node {name: 2}),"
                            + "       (nC:Node {name: 3}),"
                            + "       (nD:Node {name: 4}),"
                            + "       (nE:Node {name: 5}),"
                            + "       (nA)-[:REL]->(nB),"
                            + "       (nA)-[:REL]->(nC),"
                            + "       (nB)-[:REL]->(nE),"
                            + "       (nC)-[:REL]->(nD);")
                    .consume();
            var timestamp = tracker.getLastCommittedTime();

            // Get bfs at timestamp 2
            var bfs = session.run(String.format("CALL time.bfs(%d, %d) YIELD *", 0, timestamp))
                    .list();
            assertEquals(5, bfs.size());
            assertEquals(0L, bfs.get(0).get(2).asLong());
            assertEquals(1L, bfs.get(1).get(2).asLong());
            assertEquals(1L, bfs.get(2).get(2).asLong());
            assertEquals(2L, bfs.get(3).get(2).asLong());
            assertEquals(2L, bfs.get(4).get(2).asLong());
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {1})
    void shouldReturnBFSWithNodeDeletion(int type) throws IOException {
        // First, register the timestore listener
        registerTracker(type);

        // In a try-block, to make sure we close the session after the test
        try (Session session = driver.session()) {

            // Create a graph
            session.run("CREATE" + "  (alice:User {posts: 4, seed_label: 52}),"
                            + "  (bridget:User {posts: 13, seed_label: 21}),"
                            + "  (charles:User {posts: 55, seed_label: 43}),"
                            + "  (doug:User {posts: 5, seed_label: 21}),"
                            + "  (mark:User {posts: 7, seed_label: 19}),"
                            + "  (michael:User {posts: 15, seed_label: 52}),"
                            + "  (alice)-[:FOLLOW {weight: 10}]->(charles),"
                            + "  (alice)-[:FOLLOW {weight: 1}]->(bridget),"
                            + "  (mark)-[:FOLLOW {weight: 1}]->(doug),"
                            + "  (bridget)-[:FOLLOW {weight: 1}]->(michael),"
                            + "  (doug)-[:FOLLOW {weight: 1}]->(mark),"
                            + "  (michael)-[:FOLLOW {weight: 1}]->(alice),"
                            + "  (alice)-[:FOLLOW {weight: 1}]->(michael),"
                            + "  (bridget)-[:FOLLOW {weight: 1}]->(alice),"
                            + "  (michael)-[:FOLLOW {weight: 1}]->(bridget),"
                            + "  (charles)-[:FOLLOW {weight: 1}]->(doug);")
                    .consume();
            var timestamp1 = tracker.getLastCommittedTime();

            // Delete charles
            session.run("MATCH (n) WHERE id(n) = 2 DETACH DELETE n;").consume();
            var timestamp2 = tracker.getLastCommittedTime();

            // Get bfs at timestamp 2
            var bfs = session.run(String.format(
                            "CALL time.dynamicBfs(%d, %d, %d, %d) YIELD *", 0, timestamp1, timestamp2, timestamp1))
                    .list();
            assertEquals(11, bfs.size());
            assertEquals(0L, bfs.get(0).get(2).asLong());
            assertEquals(1L, bfs.get(1).get(2).asLong());
            assertEquals(1L, bfs.get(2).get(2).asLong());
            assertEquals(2L, bfs.get(3).get(2).asLong());
            assertEquals(3L, bfs.get(4).get(2).asLong());
            assertEquals(1L, bfs.get(5).get(2).asLong());

            assertEquals(0L, bfs.get(6).get(2).asLong());
            assertEquals(1L, bfs.get(7).get(2).asLong());
            assertEquals(Integer.MAX_VALUE, bfs.get(8).get(2).asLong());
            assertEquals(Integer.MAX_VALUE, bfs.get(9).get(2).asLong());
            assertEquals(1L, bfs.get(10).get(2).asLong());
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {1})
    void shouldReturnTemporalGraphNodes(int type) throws IOException {
        // First, register the timestore listener
        registerTracker(type);

        // In a try-block, to make sure we close the session after the test
        try (Session session = driver.session()) {

            // Create nodes 0-3
            session.run("CREATE (n1:Node {row_number: 0}), "
                            + "(n2:Node {row_number: 1}), "
                            + "(n3:Node {row_number: 2}), "
                            + "(n4:Node {row_number: 3}), "
                            + "(n5:Node {row_number: 4}), "
                            + "(n6:Node {row_number: 5}) "
                            + "RETURN n1, n2, n3, n4, n5, n6;")
                    .single();
            var timestamp1 = tracker.getLastCommittedTime();

            // Change nodes
            session.run("MATCH (n) WHERE id(n) = 2 SET n:Engineer, n.row_number = 100;")
                    .consume();
            var timestamp2 = tracker.getLastCommittedTime();

            // Change nodes
            session.run("MATCH (n) WHERE id(n) = 2 SET n:Researcher, n.row_number = 42;")
                    .consume();
            var timestamp3 = tracker.getLastCommittedTime();

            var nodes = session.run(
                            String.format("CALL time.getTemporalGraphNodes(%d, %d) YIELD *", timestamp1, timestamp3))
                    .list();

            assertEquals(8, nodes.size());
            assertEquals(0L, nodes.get(0).get(2).asLong());
            assertEquals(1L, nodes.get(1).get(2).asLong());
            assertEquals(2L, nodes.get(2).get(2).asLong());
            assertEquals(2L, nodes.get(3).get(2).asLong());
            assertEquals(2L, nodes.get(4).get(2).asLong());
            assertEquals(3L, nodes.get(5).get(2).asLong());
            assertEquals(4L, nodes.get(6).get(2).asLong());
            assertEquals(5L, nodes.get(7).get(2).asLong());
        }
    }

    public static void checkNodeExists(Session session, long nodeId, long timestamp, ResultNode expectedNode) {
        var record = session.run(String.format("CALL time.getTemporalNode(%d, %d) YIELD *", nodeId, timestamp))
                .single();

        assertEquals(record.get(0).asLong(), expectedNode.timestamp);
        assertEquals(record.get(1).asLong(), expectedNode.nodeId);
        assertEquals(record.get(2).asList(), expectedNode.labels);
        assertEquals(record.get(3).asMap(), expectedNode.properties);
    }

    private void registerTracker(int type) throws IOException {
        var dbms = embeddedDatabaseServer.databaseManagementService();
        if (type == 0) {
            tracker = new TimeBasedTracker(DEFAULT_POLICY);
        } else if (type == 1) {
            var pageCache = (PageCache) dbms.database(DEFAULT_DATABASE_NAME).getPageCache();
            var fs =
                    (FileSystemAbstraction) dbms.database(DEFAULT_DATABASE_NAME).getFileSystem();
            var logPath = directory.homePath().resolve(TIMESTORE_LOG);
            var timestorePath = directory.homePath().resolve(TIMESTORE_INDEX);

            tracker = new TimeBasedTracker(DEFAULT_POLICY, pageCache, fs, logPath, timestorePath);
        } else {
            throw new IllegalArgumentException(String.format("Type %d is not supported", type));
        }

        dbms.registerTransactionEventListener(DEFAULT_DATABASE_NAME, tracker);
    }
}
