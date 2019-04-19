#!/usr/bin/bash

# Install wget and java
#echo Install wget and java
#sudo yum -y update
#sudo yum -y install wget
#sudo yum -y install java-1.8.0-openjdk-devel
#sudo rm -f /usr/lib/jvm/jdk
#sudo ln -s $(ls -tdr1 /usr/lib/jvm/java-1.8.0-openjdk-1.8* | sort -n | tail -1) /usr/lib/jvm/jdk

#Install my stuff
sudo yum -y install tigervnc-server
sudo yum -y install xterm
#sudo yum -y install metacity
#sudo yum -y install matchbox-window-manager
sudo yum -y install links
#sudo yum -y install gcc
sudo yum -y install vim-X11
#sudo yum -y install gnome-terminal
sudo yum -y install mate-desktop
sudo yum -y install gnome-terminal
sudo yum -y install mate-terminal
sudo yum -y install mate-backgrounds
sudo yum -y install bpg-courier-fonts
sudo yum -y install wine

# Python3 and libraries:
#sudo yum -y install python3
#Python36
#sudo amazon-linux-extras install -y epel
#sudo yum -y install python36 python36-devel python36-pip
#sudo pip3 install numpy
#sudo pip3 install pandas
#sudo pip3 install matplotlib
#sudo pip3 install astral
#sudo pip3 install calmap
#sudo pip3 install jupyter

# Use a Linux editor such as vi to install the export line (below) into your ~/.bashrc:
#echo "Use a Linux editor such as vi to install the export line (below) into your ~/.bashrc:"
#vi ~/.bashrc
#-----
#export JAVA_HOME=/usr/lib/jvm/jdk
#echo "export JAVA_HOME=/usr/lib/jvm/jdk" >> ~/.bashrc
#-----

# Execute the bashrc file
#source ~/.bashrc 

# Download Spark to the ec2-user's home directory
#pushd ~
#wget http://apache.claz.org/spark/spark-2.3.1/spark-2.3.1-bin-hadoop2.7.tgz
#wget http://apache.mirrors.ionfish.org/spark/spark-2.3.3/spark-2.3.3-bin-hadoop2.7.tgz
#wget https://www-us.apache.org/dist/spark/spark-2.4.1/spark-2.4.1-bin-hadoop2.7.tgz

# Unpack Spark in the /opt directory
#sudo tar zxvf $(ls -tdr1 spark-*.tgz | tail -1) -C /opt
#popd

# Create a symbolic link to make it easier to access
#pushd /opt
#sudo rm -f /opt/spark
#sudo ln -fs $(ls -tdr1 /opt/spark-* | tail -1) /opt/spark
#popd

#-----

#sudo amazon-linux-extras install -y mate-desktop1.x

# Installing basemap and dependencies
#sudo yum -y install gcc-c++
#sudo pip3 install pillow
# Requires geos to be compiled
#export CXX="g++ -std=c++98"
#wget http://download.osgeo.org/geos/geos-3.4.2.tar.bz2
#tar xjvf geos-3.4.2.tar.bz2
#cd geos-3.4.2
#./configure
#make
#sudo make install
#cd ../
#unset CXX
#sudo pip3 install -U git+https://github.com/matplotlib/basemap.git
# It requeries an old version of pyproj
#sudo pip3 install pyproj==1.9.6
