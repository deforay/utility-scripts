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
apt-get purge --auto-remove -y php* apache2* libapache2-mod-php* phpmyadmin composer

# Stop services before removing files
systemctl stop apache2 2>/dev/null || true
systemctl stop mysql 2>/dev/null || true
systemctl disable apache2 2>/dev/null || true
systemctl disable mysql 2>/dev/null || true

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

# Remove Composer (global installations)
rm -f /usr/local/bin/composer
rm -f /usr/bin/composer
rm -rf ~/.composer
rm -rf /root/.composer
rm -rf /home/*/.composer
rm -rf /home/*/.config/composer

# Remove switch-php script
rm -f /usr/local/bin/switch-php

# Remove PHP alternatives
update-alternatives --remove-all php 2>/dev/null || true
update-alternatives --remove-all phar 2>/dev/null || true
update-alternatives --remove-all phar.phar 2>/dev/null || true
update-alternatives --remove-all phpize 2>/dev/null || true
update-alternatives --remove-all php-config 2>/dev/null || true

# Remove Apache systemd override files
rm -rf /etc/systemd/system/apache2.service.d/
rm -rf /etc/systemd/system/mysql.service.d/
systemctl daemon-reload

# Remove user/group if they exist (optional - uncomment if desired)
# userdel -r mysql 2>/dev/null || true
# groupdel mysql 2>/dev/null || true

# Clean up APT cache and package lists
rm -rf /var/lib/apt/lists/ppa.launchpad.net_ondrej_*
rm -rf /var/cache/apt/archives/php*
rm -rf /var/cache/apt/archives/mysql*
rm -rf /var/cache/apt/archives/apache2*
rm -rf /var/cache/apt/archives/libapache2*

# Remove apt preferences for Ondřej PPA
rm -f /etc/apt/preferences.d/ondrej-php.pref

# Remove Ondřej PPAs
add-apt-repository --remove -y ppa:ondrej/php 2>/dev/null || true
add-apt-repository --remove -y ppa:ondrej/apache2 2>/dev/null || true
rm -f /etc/apt/sources.list.d/ondrej-ubuntu-php*.list
rm -f /etc/apt/sources.list.d/ondrej-ubuntu-apache2*.list

# Update apt cache after removing PPAs
apt-get update

# Clean up orphaned packages and residual config files
apt-get autoremove -y
apt-get autoclean -y

# Verify removal
dpkg -l | grep -E 'mysql|apache2|php|phpmyadmin'

echo "Purge and cleanup completed. Please review the output above for any remaining packages."
