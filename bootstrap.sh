#!/usr/bin/env bash

apt-get update
apt-get install -y openjdk-7-jdk leiningen maven tmux git vim

groupadd storm
useradd --gid storm --home-dir /home/storm --create-home --shell /bin/bash storm

chown -R storm:storm /opt/storm-multicore
ln -s /opt/storm-multicore/bin/storm /usr/bin/storm

mkdir /etc/storm
chown storm:storm /etc/storm

mkdir /var/log/storm
chown storm:storm /var/log/storm
