# Upgrade and download
sudo apt-get update
sudo apt-get -y upgrade

# Java 8
sudo apt-get -y install openjdk-8-jdk

# Window manager for vnc:
sudo apt-get -y install xfce4 vnc4server

#gnome
sudo apt-get -y install xfce4 gnome-session
sudo apt-get -y install xfce4 gnome-panel
sudo apt-get -y install xfce4 gnome-settings-daemon

# gvim:
sudo apt-get -y install vim-gtk3

# Python (needed for crawler):
sudo apt-get -y install python3-pip

# Python packages
pip3 install pandas




