#!/usr/bin/env bash

# Tested on Amazon linux machines

set -euxo pipefail

OPT_DIR='/opt/apps'
PYENV="${OPT_DIR}/pyenv"

mkdir -p $OPT_DIR
chown "$SUDO_USER" $OPT_DIR
su - "$SUDO_USER" -c "python3 -m virtualenv -p python3.9 $PYENV"