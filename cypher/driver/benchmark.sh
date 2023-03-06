#!/bin/bash

set -eu
set -o pipefail

cd "$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd ..

BENCHMARK_PROPERTIES_FILE=${1:-driver/benchmark.properties}

java \
         --add-opens=java.base/java.lang=ALL-UNNAMED \
         --add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
         --add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
         --add-opens=java.base/java.io=ALL-UNNAMED \
         --add-opens=java.base/java.net=ALL-UNNAMED \
         --add-opens=java.base/java.nio=ALL-UNNAMED \
         --add-opens=java.base/java.util=ALL-UNNAMED \
         --add-opens=java.base/java.util.concurrent=ALL-UNNAMED \
         --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED \
         --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
         --add-opens=java.base/sun.nio.cs=ALL-UNNAMED \
         --add-opens=java.base/sun.security.action=ALL-UNNAMED \
         --add-opens=java.base/sun.util.calendar=ALL-UNNAMED \
         --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED \
         -cp target/cypher-1.0.0.jar org.ldbcouncil.snb.driver.Client -P ${BENCHMARK_PROPERTIES_FILE}
