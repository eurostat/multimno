#!/bin/bash

# Versions
# Select Python version
python_version=3.11
min_python_version=3.8

# Select Spark version
spark_version="3.5.1"
# Select Scala version
scala_version="2.12"
# Select Sedona version
sedona_version="1.6.1"
geotools_wrapper_version="28.2"

# Paths
upload_dir="upload"
code_dir="$upload_dir/code"
env_dir="$upload_dir/env"

# Funcions
usage () {
    echo "Usage: $0 [-d] [-s] [-p python_version] [-v] [-h]"
    echo "Options:"
    echo "  -d: Download dependencies"
    echo "  -p: Python version (default: $python_version) (min: $min_python_version)"
    echo "  -s: Download pyspark python dependencies. Only works with -d option"
    echo "  -j: Download spark jars dependencies"
    echo "  -v: Verbose"
    echo "  -h: Help"
    echo "Examples:"
    echo "  $0 # Only generate whl and orchestrator"
    echo "  $0 -d # Download dependencies and generate whl and orchestrator"
    echo "  $0 -d -p 3.8 -v # Download dependencies for python 3.8 and verbose"
}

download_python_deps() {
    python_deps_dir=$1
    python_version=$2

    mkdir -p $python_deps_dir
    # Download static libraries with pip
    echo "[*] Downloading python dependencies..."
    tmp_requirements_path=/tmp/multimno_requirements.txt

    if [ -z "$download_spark" ]; then
        optional_lists=""
    else
        optional_lists="--optional-lists $download_spark"
    fi

    python${python_version} -m pip install toml-to-requirements $download_spark > $log_descriptor 2>&1
    toml-to-req --toml-file pyproject.toml --requirements-file $tmp_requirements_path $optional_lists > $log_descriptor 2>&1
    python${python_version} -m pip download -d $python_deps_dir -r $tmp_requirements_path > $log_descriptor 2>&1
    if [ $? -ne 0 ]; then
        echo "[!] Error downloading python dependencies"
        exit 1
    fi
}

# Default values
download_deps=false
download_jars=false
download_spark="" 
log_descriptor="/dev/null"

# Arguments parse
while getopts "dhp:sjv" opt; do
    case ${opt} in
        d)
            download_deps=true ;;
        h)
            usage
            exit 0 ;;
        p)
            python_version="${OPTARG}" ;;
        s) 
            download_spark="spark" ;;
        j)
            download_jars=true ;;
        v) 
            log_descriptor="/dev/stdout" ;;
        \?)
            echo "Invalid option: -${OPTARG}" >&2
            usage
            exit 1 ;;
        :)
            echo "Option -${OPTARG} requires an argument." >&2
            exit 1 ;;
    esac
done

# --- Main ---
echo "[START] Generating deployment package..."

# Create code dir
mkdir -p $code_dir

# 0) Check if python version is installed

is_python_version_installed=$(python${python_version} --version > /dev/null 2>&1; echo $?)

# If python version is not installed, download it
if [ "$is_python_version_installed" != 0 ]; then
    echo "[!] Python $python_version is not installed. Downloading..."
    # Download python
    apt update > $log_descriptor 2>&1
    apt install -y python$python_version python$python_version-distutils > $log_descriptor 2>&1
    # Add pip to downloaded python
    curl https://bootstrap.pypa.io/get-pip.py | python${python_version}
    # Install build
    python${python_version} -m pip install --upgrade build > $log_descriptor 2>&1
fi

# 1) Add dependencies if required

# Download jars
if [ "$download_jars" = true ]; then
    echo "[*] Adding jars dependencies..."
    jars_deps_dir="$env_dir/jars"
    mkdir -p $jars_deps_dir
    ./resources/scripts/install_sedona_jars.sh ${spark_version} ${scala_version} ${sedona_version} ${geotools_wrapper_version} $jars_deps_dir > $log_descriptor 2>&1
    if [ $? -ne 0 ]; then
        echo "[!] Error downloading jars"
        exit 1
    fi
fi

# Download python deps
if [ "$download_deps" = true ]; then
    echo "[*] Adding python dependencies..."
    python_deps_dir="$env_dir/python_dependencies"
    download_python_deps $python_deps_dir $python_version
else
    echo "[*] Skipping dependencies download..."
fi

# 2) Add code

# get requirements path and set it in the env var

# Compile code setuptools
echo "[*] Compiling code..."
rm -rf build dist
python${python_version} -m build > $log_descriptor 2>&1
if [ $? -ne 0 ]; then
    echo "[!] Error compiling code"
    exit 1
fi


echo "[*] Adding source code..."
whl_file=$(ls -t1 dist | grep -E "^multimno.*whl$" | head -n 1)
cp dist/$whl_file $code_dir/$whl_file
cp multimno/main_multimno.py $code_dir/main_multimno.py
cp multimno/orchestrator_multimno.py $code_dir/orchestrator_multimno.py

echo "[END] Deployment package generated successfully in $(realpath $upload_dir)"