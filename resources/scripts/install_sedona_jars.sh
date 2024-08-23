#!/bin/bash

# Define variables
spark_minor_version=${1%.*}
scala_version=$2
sedona_version=$3
geotools_wrapper_version=$4

download_dir=$5
if [ -z "$download_dir" ]; then
  download_dir=$SPARK_HOME/jars
fi

mkdir -p $download_dir

gw_version="$sedona_version-$geotools_wrapper_version"

sedona_maven_url="https://repo1.maven.org/maven2/org/apache/sedona"
geotools_maven_url="https://repo1.maven.org/maven2/org/datasyslab/geotools-wrapper"

# Download Sedona
curl ${sedona_maven_url}/sedona-spark-shaded-${spark_minor_version}_${scala_version}/${sedona_version}/sedona-spark-shaded-${spark_minor_version}_${scala_version}-${sedona_version}.jar -o $download_dir/sedona-spark-shaded-${spark_minor_version}_${scala_version}-${sedona_version}.jar
curl ${geotools_maven_url}/${gw_version}/geotools-wrapper-${gw_version}.jar -o $download_dir/geotools-wrapper-${gw_version}.jar