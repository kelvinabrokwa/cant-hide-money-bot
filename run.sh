#!/usr/bin/env bash

set -euxo pipefail

# Write all stdout and stderr to syslog
exec 1> >(logger -s -t "$(basename $0)") 2>&1

while :
do
  /opt/apps/pyenv/bin/python -m cant_hide_money_bot.server_bot --mode prod
  sleep 2
done