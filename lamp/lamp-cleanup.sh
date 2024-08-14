#!/bin/bash

# To use this script:
# sudo -s
# cd /tmp
# wget https://raw.githubusercontent.com/deforay/utility-scripts/master/lamp/lamp-cleanup.sh
# chmod +x ./lamp-cleanup.sh
# ./lamp-cleanup.sh

# Ensure the script is run as sudo or root
if [ "$EUID" -ne 0 ]; then
    echo "Please run as root or use sudo"
    exit 1
fi

# Purge MySQL, PHP, Apache, and phpMyAdmin
apt-get purge --auto-remove -y mysql-server mysql-client mysql-common mysql-server-core-* mysql-client-core-*
apt-get purge --auto-remove -y php* apache2* libapache2-mod-php* phpmyadmin

# Stop services before removing files
systemctl stop apache2
systemctl stop mysql

# Delete associated directories
rm -rf /var/www/
rm -rf /etc/apache2/
rm -rf /var/lib/mysql/
rm -rf /var/log/mysql/
rm -rf /etc/mysql/
rm -rf /usr/share/phpmyadmin/
rm -rf /var/lib/phpmyadmin/
rm -rf /var/log/apache2/
rm -rf /etc/php/

# Clean up orphaned packages and residual config files
apt-get autoremove -y
apt-get autoclean -y

# Verify removal
dpkg -l | grep -E 'mysql|apache2|php|phpmyadmin'

echo "Purge and cleanup completed. Please review the output above for any remaining packages."
