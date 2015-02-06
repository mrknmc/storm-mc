#!/usr/bin/env bash

# Make sure necessities are there
apt-get update
apt-get install -y openjdk-7-jdk maven tmux git vim tree

# Install Leiningen
wget https://raw.githubusercontent.com/technomancy/leiningen/stable/bin/lein
chmod a+x lein
mv lein /usr/bin
/usr/bin/lein

# Create `storm` user
groupadd storm
useradd --gid storm --home-dir /home/storm --create-home --shell /bin/bash storm
chown -R storm:storm /opt/storm-mc
chown -R storm:storm /opt/storm
ln -s /opt/storm-mc/bin/storm /usr/bin/storm-mc
ln -s /opt/storm/bin/storm /usr/bin/storm


# Make etc directories
mkdir /etc/storm-mc
mkdir /etc/storm
chown storm:storm /etc/storm-mc
chown storm:storm /etc/storm


# Make log directories
mkdir /var/log/storm-mc
mkdir /var/log/storm
chown storm:storm /var/log/storm-mc
chown storm:storm /var/log/storm
