#!/usr/bin/env bash
set -o nounset -o pipefail -o errexit

echo '::group::apt-get update'
printf 'deb mirror://mirrors.ubuntu.com/mirrors.txt %s main restricted universe multiverse\n' \
  "$(lsb_release -cs)" | sudo tee -a /etc/apt/sources.list
sudo --non-interactive apt-get update
echo '::endgroup::'

echo '::group::apt-get install'
sudo --non-interactive apt-get install -y \
  bsdmainutils docker-compose jq
echo '::endgroup::'
