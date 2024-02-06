#!/usr/bin/env bash

if [ -z "${USER_HOME}" ]; then
  echo "Please set USER_HOME environment variable"
  return
fi
if [ -z "${NEO4J_HOME}" ]; then
  echo "Please set NEO4J_HOME environment variable"
  return
fi
if [ -z "${JAVA_HOME}" ]; then
  echo "Please set JAVA_HOME environment variable"
  return
fi

ITERATIONS=(1)
# shellcheck disable=SC2206
DATASETS=(
  "DBLP"
  "WIKITALK"
  "POKEC"
  "LIVEJOURNAL"
)

# Create result file and add its header
result_file_name="expand_res"
current_time=$(date "+%Y.%m.%d-%H.%M.%S")
new_fileName=$result_file_name.$current_time.csv

echo "Creating csv $new_fileName and adding its header"
echo "DATASET_NAME, WORKLOAD, COL1, COL2, COL3, COL4" >> $new_fileName

echo "Running experiments for regular execution"
for dataset in "${DATASETS[@]}"; do
  # shellcheck disable=SC2034
  for iter in "${!ITERATIONS[@]}"; do
    ${JAVA_HOME}/bin/java -Xms60g -Xmx60g \
    --add-exports jdk.javadoc/jdk.javadoc.internal.tool=ALL-UNNAMED -Dfile.encoding=UTF-8 -classpath ${NEO4J_HOME}/community/temporal-graph/target/classes:${NEO4J_HOME}/community/gbptree-tests/target/classes:${NEO4J_HOME}/community/index/target/classes:${USER_HOME}/.m2/repository/org/apache/commons/commons-lang3/3.12.0/commons-lang3-3.12.0.jar:${NEO4J_HOME}/community/graphdb-api/target/classes:${NEO4J_HOME}/annotations/target/classes:${NEO4J_HOME}/community/common/target/classes:${USER_HOME}/.m2/repository/org/lz4/lz4-java/1.8.0/lz4-java-1.8.0.jar:${USER_HOME}/.m2/repository/org/apache/commons/commons-text/1.10.0/commons-text-1.10.0.jar:${NEO4J_HOME}/community/resource/target/classes:${NEO4J_HOME}/community/collections/target/classes:${USER_HOME}/.m2/repository/com/github/ben-manes/caffeine/caffeine/3.1.4/caffeine-3.1.4.jar:${NEO4J_HOME}/community/io/target/classes:${NEO4J_HOME}/community/unsafe/target/classes:${USER_HOME}/.m2/repository/net/java/dev/jna/jna/5.13.0/jna-5.13.0.jar:${NEO4J_HOME}/community/native/target/classes:${NEO4J_HOME}/community/concurrent/target/classes:${NEO4J_HOME}/community/kernel/target/classes:${NEO4J_HOME}/community/kernel-api/target/classes:${NEO4J_HOME}/community/procedure-api/target/classes:${NEO4J_HOME}/community/monitoring/target/classes:${NEO4J_HOME}/community/diagnostics/target/classes:${NEO4J_HOME}/community/schema/target/classes:${NEO4J_HOME}/community/lock/target/classes:${NEO4J_HOME}/community/token-api/target/classes:${NEO4J_HOME}/community/storage-engine-util/target/classes:${NEO4J_HOME}/community/values/target/classes:${NEO4J_HOME}/community/neo4j-exceptions/target/classes:${NEO4J_HOME}/community/logging/target/classes:${USER_HOME}/.m2/repository/org/codehaus/jettison/jettison/1.5.4/jettison-1.5.4.jar:${USER_HOME}/.m2/repository/org/apache/logging/log4j/log4j-api/2.20.0/log4j-api-2.20.0.jar:${USER_HOME}/.m2/repository/org/apache/logging/log4j/log4j-core/2.20.0/log4j-core-2.20.0.jar:${USER_HOME}/.m2/repository/org/apache/logging/log4j/log4j-layout-template-json/2.20.0/log4j-layout-template-json-2.20.0.jar:${NEO4J_HOME}/community/configuration/target/classes:${USER_HOME}/.m2/repository/com/github/seancfoley/ipaddress/5.4.0/ipaddress-5.4.0.jar:${NEO4J_HOME}/community/layout/target/classes:${NEO4J_HOME}/community/spatial-index/target/classes:${NEO4J_HOME}/community/fulltext-index/target/classes:${NEO4J_HOME}/community/lucene-index/target/classes:${USER_HOME}/.m2/repository/org/apache/lucene/lucene-analysis-common/9.5.0/lucene-analysis-common-9.5.0.jar:${USER_HOME}/.m2/repository/org/apache/lucene/lucene-core/9.5.0/lucene-core-9.5.0.jar:${USER_HOME}/.m2/repository/org/apache/lucene/lucene-queryparser/9.5.0/lucene-queryparser-9.5.0.jar:${USER_HOME}/.m2/repository/org/apache/lucene/lucene-backward-codecs/9.5.0/lucene-backward-codecs-9.5.0.jar:${NEO4J_HOME}/community/id-generator/target/classes:${NEO4J_HOME}/community/wal/target/classes:${NEO4J_HOME}/community/import-util/target/classes:${NEO4J_HOME}/community/csv/target/classes:${USER_HOME}/.m2/repository/org/jctools/jctools-core/4.0.1/jctools-core-4.0.1.jar:${USER_HOME}/.m2/repository/commons-io/commons-io/2.11.0/commons-io-2.11.0.jar:${USER_HOME}/.m2/repository/org/apache/commons/commons-math3/3.6.1/commons-math3-3.6.1.jar:${NEO4J_HOME}/community/server-api/target/classes:${USER_HOME}/.m2/repository/jakarta/ws/rs/jakarta.ws.rs-api/2.1.6/jakarta.ws.rs-api-2.1.6.jar:${USER_HOME}/.m2/repository/org/roaringbitmap/RoaringBitmap/0.9.47/RoaringBitmap-0.9.47.jar:${USER_HOME}/.m2/repository/org/roaringbitmap/shims/0.9.47/shims-0.9.47.jar:${USER_HOME}/.m2/repository/org/eclipse/collections/eclipse-collections-api/11.1.0/eclipse-collections-api-11.1.0.jar:${USER_HOME}/.m2/repository/org/eclipse/collections/eclipse-collections/11.1.0/eclipse-collections-11.1.0.jar:${USER_HOME}/.m2/repository/com/carrotsearch/hppc/0.7.3/hppc-0.7.3.jar:${USER_HOME}/.m2/repository/org/openjdk/jmh/jmh-core/1.35/jmh-core-1.35.jar:${USER_HOME}/.m2/repository/net/sf/jopt-simple/jopt-simple/5.0.4/jopt-simple-5.0.4.jar:${USER_HOME}/.m2/repository/org/openjdk/jmh/jmh-generator-annprocess/1.35/jmh-generator-annprocess-1.35.jar org.neo4j.temporalgraph.benchmarks.TemporalGraphBenchmark \
    $dataset 1 >> $new_fileName

  done
done

echo "Done"
