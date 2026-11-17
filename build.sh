#!/usr/bin/env bash
# Builds minisqs.jar using only the JDK (javac + jar). Needs JDK 21+.
set -euo pipefail
cd "$(dirname "$0")"
rm -rf out minisqs.jar
mkdir out
javac -d out $(find src/main/java -name '*.java')
printf 'Main-Class: com.minisqs.Cli\n' > out/manifest.txt
jar --create --file minisqs.jar --manifest out/manifest.txt -C out com
echo "Built minisqs.jar"
