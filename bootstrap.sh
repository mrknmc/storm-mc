#!/usr/bin/env bash

apt-get update
apt-get install -y openjdk-7-jdk maven tmux git vim tree
wget https://raw.githubusercontent.com/technomancy/leiningen/stable/bin/lein
chmod a+x lein
mv lein /usr/bin
/usr/bin/lein

groupadd storm
useradd --gid storm --home-dir /home/storm --create-home --shell /bin/bash storm

chown -R storm:storm /opt/storm-multicore
ln -s /opt/storm-multicore/bin/storm /usr/bin/storm

mkdir /etc/storm
chown storm:storm /etc/storm

mkdir /var/log/storm
chown storm:storm /var/log/storm
