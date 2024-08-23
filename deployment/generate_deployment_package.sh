#!/bin/bash

# Versions
# Select Python version
python_version=3.11
min_python_version=3.8

# Select Spark version
spark_version=$SPARK_VERSION
# Select Scala version
scala_version=$SCALA_VERSION
# Select Sedona version
sedona_version=$SEDONA_VERSION
geotools_wrapper_version=$GEOTOOLS_WRAPPER_VERSION

# Paths
upload_dir="upload"
code_dir="$upload_dir/code"

# Funcions
usage () {
    echo "Usage: $0 [-d] [-s] [-p python_version] [-v] [-h]"
    echo "Options:"
    echo "  -d: Download dependencies"
    echo "  -p: Python version (default: $python_version) (min: $min_python_version)"
    echo "  -s: Download pyspark dependencies. Only works with -d option"
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

    python${python_version} -m pip install toml-to-requirements $download_spark > $log_descriptor 2>&1
    toml-to-req --toml-file pyproject.toml --requirements-file $tmp_requirements_path --optional-lists $download_spark > $log_descriptor 2>&1
    python${python_version} -m pip download -d $python_deps_dir -r $tmp_requirements_path > $log_descriptor 2>&1
    if [ $? -ne 0 ]; then
        echo "[!] Error downloading python dependencies"
        exit 1
    fi
}

# Default values
download_deps=false
download_spark="" 
log_descriptor="/dev/null"

# Arguments parse
while getopts "dhp:sv" opt; do
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
if [ "$download_deps" = true ]; then
    # add jars
    echo "[*] Adding jars dependencies..."
    jars_deps_dir="$code_dir/jars"
    mkdir -p $jars_deps_dir
    ./resources/scripts/install_sedona_jars.sh ${SPARK_VERSION} ${SCALA_VERSION} ${SEDONA_VERSION} ${GEOTOOLS_WRAPPER_VERSION} $jars_deps_dir > $log_descriptor 2>&1
    if [ $? -ne 0 ]; then
        echo "[!] Error downloading jars"
        exit 1
    fi
    # add python deps
    echo "[*] Adding python dependencies..."
    python_deps_dir="$code_dir/python_dependencies"
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