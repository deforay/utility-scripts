#!/bin/bash

# To use this script:
# sudo -s
# cd /tmp
# wget https://raw.githubusercontent.com/deforay/utility-scripts/master/lamp/lamp-cleanup.sh
# chmod +x ./lamp-cleanup.sh
# ./lamp-cleanup.sh

# Purge MySQL, PHP, Apache, and phpMyAdmin
sudo apt-get purge --auto-remove -y mysql-server mysql-client mysql-common mysql-server-core-* mysql-client-core-*
sudo apt-get purge --auto-remove -y php* apache2* libapache2-mod-php* phpmyadmin

# Stop services before removing files
sudo systemctl stop apache2
sudo systemctl stop mysql

# Delete associated directories
sudo rm -rf /var/www/
sudo rm -rf /etc/apache2/
sudo rm -rf /var/lib/mysql/
sudo rm -rf /var/log/mysql/
sudo rm -rf /etc/mysql/
sudo rm -rf /usr/share/phpmyadmin/
sudo rm -rf /var/lib/phpmyadmin/
sudo rm -rf /var/log/apache2/
sudo rm -rf /etc/php/

# Clean up orphaned packages and residual config files
sudo apt-get autoremove -y
sudo apt-get autoclean -y

# Verify removal
dpkg -l | grep -E 'mysql|apache2|php|phpmyadmin'

echo "Purge and cleanup completed. Please review the output above for any remaining packages."
