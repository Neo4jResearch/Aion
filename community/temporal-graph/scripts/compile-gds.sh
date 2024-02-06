#!/bin/bash

## Install sdk, java, maven, and gradle
# curl -s "https://get.sdkman.io" | bash
# source "$HOME/.sdkman/bin/sdkman-init.sh"
# sdk install java 11.0.19-tem
# sdk install java 17.0.7-tem
# sdk install gradle 8.5

## Use java 17 and gradle 8+
git clone https://github.com/neo4j/graph-data-science.git
cd graph-data-science
git checkout 2.4.0-alpha06
git apply ../temporal.patch
./gradlew :open-packaging:shadowCopy -Pneo4jVersion=5.7.0
./gradlew publishToMavenLocal