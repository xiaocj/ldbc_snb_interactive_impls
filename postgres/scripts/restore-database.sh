#!/bin/bash

set -eu
set -o pipefail

cd "$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd ..

. scripts/vars.sh

scripts/stop.sh
sudo rm -rf scratch/data/
sudo cp -r scratch/backup/ scratch/data/
scripts/start.sh
