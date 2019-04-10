# Install wget and java
echo Install wget and java
sudo yum -y update
sudo yum -y install wget
sudo yum -y install java-1.8.0-openjdk-devel
sudo rm -f /usr/lib/jvm/jdk
sudo ln -s $(ls -tdr1 /usr/lib/jvm/java-1.8.0-openjdk-1.8* | sort -n | tail -1) /usr/lib/jvm/jdk

#Install my stuff
sudo yum -y install tigervnc-server
sudo yum -y install xterm
sudo yum -y install metacity
sudo yum -y install links


# Use a Linux editor such as vi to install the export line (below) into your ~/.bashrc:
echo "Use a Linux editor such as vi to install the export line (below) into your ~/.bashrc:"
#vi ~/.bashrc
#-----
export JAVA_HOME=/usr/lib/jvm/jdk
echo "export JAVA_HOME=/usr/lib/jvm/jdk" >> ~/.bashrc
#-----

# Execute the bashrc file
#source ~/.bashrc 

# Download Spark to the ec2-user's home directory
pushd ~
#wget http://apache.claz.org/spark/spark-2.3.1/spark-2.3.1-bin-hadoop2.7.tgz
wget http://apache.mirrors.ionfish.org/spark/spark-2.3.3/spark-2.3.3-bin-hadoop2.7.tgz
popd

# Unpack Spark in the /opt directory
sudo tar zxvf $(ls -tdr1 spark-*.tgz | tail -1) -C /opt

# Create a symbolic link to make it easier to access
pushd /opt
sudo rm -f /opt/spark
sudo ln -fs $(ls -tdr1 /opt/spark-* | tail -1) /opt/spark
popd

#-----
echo "export SPARK_HOME=/opt/spark" >> ~/.bashrc
echo "PATH=$PATH:$SPARK_HOME/bin" >> ~/.bashrc
echo "export PATH" >> ~/.bashrc

