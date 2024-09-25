#!/bin/bash

# Check if the DATA_PATH argument is provided
if [ "$#" -ne 1 ]; then
    echo "Usage: \$0 <DATA_PATH>"
    exit 1
fi

DATA_PATH=$1

# Install ossutil
echo "Installing ossutil..."
curl -s https://gosspublic.alicdn.com/ossutil/install.sh | bash

# Download data from OSS
echo "Downloading data from OSS to $DATA_PATH..."
ossutil cp -r oss://graphscope/graphar_artifact "$DATA_PATH"

echo "Download complete."