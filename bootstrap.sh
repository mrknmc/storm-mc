#!/usr/bin/env bash

# Make sure necessities are there
apt-get update
apt-get install -y openjdk-7-jdk maven tmux git vim tree

# Required for profiling tools
apt-get install -y subversion make g++

# Install Leiningen
wget https://raw.githubusercontent.com/technomancy/leiningen/stable/bin/lein
chmod a+x lein
mv lein /usr/bin
/usr/bin/lein

# Build Storm-MC release
mkdir /opt/storm-mc
cd /home/vagrant/storm-mc
mvn clean install -DskipTests=true
cd ./storm-dist/binary
mvn clean package 
cd ./target
tar -xzvf storm-mc-0.1.1.tar.gz -C /opt/storm-mc
ln -s /opt/storm-mc/bin/storm-mc /usr/bin/storm-mc
chown -R vagrant:vagrant /opt/storm-mc

# Build Storm release
mkdir /opt/storm
cd /home/vagrant/storm
mvn clean install -DskipTests=true
cd ./storm-dist/binary
mvn clean package 
cd ./target
tar -xzvf apache-storm-0.9.3.tar.gz -C /opt/storm
ln -s /opt/storm/bin/storm /usr/bin/storm
chown -R vagrant:vagrant /opt/storm

# Make etc directories
mkdir /etc/storm-mc
mkdir /etc/storm
chown vagrant:vagrant /etc/storm-mc
chown vagrant:vagrant /etc/storm

# Make log directories
mkdir /var/log/storm-mc
mkdir /var/log/storm
chown vagrant:vagrant /var/log/storm-mc
chown vagrant:vagrant /var/log/storm
