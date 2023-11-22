#!/bin/bash

# Define variables
sedona_version=$1
geotools_wrapper_version=$2
spark_minor_version=${3%.*}

gw_version="$sedona_version-$geotools_wrapper_version"

# Download Sedona
curl https://repo1.maven.org/maven2/org/apache/sedona/sedona-spark-shaded-${spark_minor_version}_2.12/${sedona_version}/sedona-spark-shaded-${spark_minor_version}_2.12-${sedona_version}.jar -o $SPARK_HOME/jars/sedona-spark-shaded-${spark_minor_version}_2.12-${sedona_version}.jar
curl https://repo1.maven.org/maven2/org/datasyslab/geotools-wrapper/${gw_version}/geotools-wrapper-${gw_version}.jar -o $SPARK_HOME/jars/geotools-wrapper-${gw_version}.jar
