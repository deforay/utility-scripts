#!/bin/bash

# To use this script:
# sudo -s
# cd /tmp
# wget https://raw.githubusercontent.com/deforay/utility-scripts/master/lamp/lamp-setup.sh
# chmod +x ./lamp-setup.sh
# ./lamp-setup.sh [PHP_VERSION]

# Check if running as root
if [ "$EUID" -ne 0 ]; then
    echo "Need admin privileges for this script. Run sudo -s before running this script or run this script with sudo"
    exit 1
fi

ask_yes_no() {
    local timeout=15
    local default=${2:-"no"} # set default value from the argument, fallback to "no" if not provided
    local answer=""

    while true; do
        echo -n "$1 (y/n): "
        read -t $timeout answer
        if [ $? -ne 0 ]; then
            answer=$default
        fi

        answer=$(echo "$answer" | awk '{print tolower($0)}')
        case "$answer" in
        "yes" | "y") return 0 ;;
        "no" | "n") return 1 ;;
        *)
            if [ -z "$answer" ]; then
                # If no input is given and it times out, apply the default value
                if [ "$default" == "yes" ] || [ "$default" == "y" ]; then
                    return 0
                else
                    return 1
                fi
            else
                echo "Invalid response. Please answer 'yes/y' or 'no/n'."
            fi
            ;;
        esac
    done
}

spinner() {
    local pid=$!
    local delay=0.1
    local spinstr='|/-\'
    while [ "$(ps a | awk '{print $1}' | grep $pid)" ]; do
        local temp=${spinstr#?}
        printf " [%c]  " "$spinstr"
        local spinstr=$temp${spinstr%"$temp"}
        sleep $delay
        printf "\b\b\b\b\b\b"
    done
    printf "    \b\b\b\b"
}

# Function to log messages
log_action() {
    local message=$1
    echo "$(date +'%Y-%m-%d %H:%M:%S') - $message" >>~/logsetup.log
}

error_handling() {
    local last_cmd=$1
    local last_line=$2
    local last_error=$3
    echo "Error on or near line ${last_line}; command executed was '${last_cmd}' which exited with status ${last_error}"
    log_action "Error on or near line ${last_line}; command executed was '${last_cmd}' which exited with status ${last_error}"

    # Check if the error is critical
    if [ "$last_error" -eq 1 ]; then # Adjust according to the error codes you consider critical
        echo "This error is critical, exiting..."
        exit 1
    else
        echo "This error is not critical, continuing..."
    fi
}

# Error trap
trap 'error_handling "${BASH_COMMAND}" "$LINENO" "$?"' ERR

# Check if Ubuntu version is 20.04 or newer
min_version="22.04"
current_version=$(lsb_release -rs)

if [[ "$(printf '%s\n' "$min_version" "$current_version" | sort -V | head -n1)" != "$min_version" ]]; then
    echo "This script is not compatible with Ubuntu versions older than ${min_version}."
    exit 1
fi

# Check for dependencies
for cmd in "apt-get"; do
    if ! command -v $cmd &>/dev/null; then
        echo "$cmd is not installed. Exiting..."
        exit 1
    fi
done

# Initial Setup
# Update Ubuntu Packages
echo "Updating Ubuntu packages..."
apt-get update && apt-get upgrade -y

# Configure any packages that were not fully installed
echo "Configuring any partially installed packages..."
dpkg --configure -a

# Clean up
apt-get autoremove -y

echo "Installing basic packages..."
apt-get install -y build-essential software-properties-common gnupg apt-transport-https ca-certificates lsb-release wget vim zip unzip curl acl snapd rsync git gdebi net-tools sed mawk magic-wormhole openssh-server libsodium-dev mosh

if ! grep -q "ondrej/apache2" /etc/apt/sources.list /etc/apt/sources.list.d/*; then
    add-apt-repository ppa:ondrej/apache2 -y
fi

echo "Setting up locale..."
locale-gen en_US en_US.UTF-8
export LANG=en_US.UTF-8
export LC_ALL=en_US.UTF-8
update-locale

# Check if SSH service is enabled
if ! systemctl is-enabled ssh >/dev/null 2>&1; then
    echo "Enabling SSH service..."
    systemctl enable ssh
else
    echo "SSH service is already enabled."
fi

# Check if SSH service is running
if ! systemctl is-active ssh >/dev/null 2>&1; then
    echo "Starting SSH service..."
    systemctl start ssh
else
    echo "SSH service is already running."
fi

# Apache Setup
if command -v apache2 &>/dev/null; then
    echo "Apache is already installed. Skipping installation..."
else
    echo "Installing and configuring Apache..."
    apt-get install -y apache2
    a2dismod mpm_event
    a2enmod rewrite headers deflate env mpm_prefork

    service apache2 restart || {
        echo "Failed to restart Apache2. Exiting..."
        exit 1
    }
fi

# Check for Brotli support and install it if necessary
if ! apache2ctl -M | grep -q 'brotli_module'; then
    echo "Installing Brotli module for Apache..."
    log_action "Installing Brotli module for Apache..."
    apt-get install -y brotli

    if [ $? -eq 0 ]; then
        echo "Enabling Brotli module..."
        a2enmod brotli
        service apache2 restart || {
            echo "Failed to restart Apache after enabling Brotli. Exiting..."
            exit 1
        }
    else
        echo "Failed to install Brotli module. Continuing without Brotli support..."
        log_action "Failed to install Brotli module. Continuing without Brotli support..."
    fi
else
    echo "Brotli module is already installed and enabled."
    log_action "Brotli module is already installed and enabled."
fi

setfacl -R -m u:$USER:rwx,u:www-data:rwx /var/www

# Prompt for MySQL root password and configure mysql_config_editor
mysql_root_password=""
mysql_root_password_confirm=""
while :; do # Infinite loop to keep asking until a correct password is provided
    while [ -z "${mysql_root_password}" ] || [ "${mysql_root_password}" != "${mysql_root_password_confirm}" ]; do
        read -sp "Please enter the MySQL root password (cannot be blank): " mysql_root_password
        echo
        read -sp "Please confirm the MySQL root password: " mysql_root_password_confirm
        echo

        if [ -z "${mysql_root_password}" ]; then
            echo "Password cannot be blank."
        elif [ "${mysql_root_password}" != "${mysql_root_password_confirm}" ]; then
            echo "Passwords do not match. Please try again."
        fi
    done

    echo "Configuring MySQL root password in ~/.my.cnf..."
    cat <<EOF >~/.my.cnf
[client]
user=root
password=${mysql_root_password}
host=localhost
EOF
    chmod 600 ~/.my.cnf

    # Verify MySQL password
    if command -v mysql &>/dev/null; then
        echo "MySQL is already installed. Verifying password..."
        if mysqladmin ping -u root -p"${mysql_root_password}" &>/dev/null; then
            echo "Password verified."

            echo "MySQL credentials configured successfully in ~/.my.cnf."
            break # Exit the loop if the password is correct
        else
            echo "Password incorrect or MySQL server unreachable. Please try again."
            mysql_root_password="" # Reset password variables to prompt again
            mysql_root_password_confirm=""
        fi
    else
        echo "MySQL is not installed. Installing MySQL..."
        apt-get install -y mysql-server

        # Set MySQL root password and configure secure login
        echo "Setting MySQL root password..."
        mysql -e "ALTER USER 'root'@'localhost' IDENTIFIED WITH mysql_native_password BY '${mysql_root_password}'; FLUSH PRIVILEGES;"

        # Restart MySQL to apply changes
        service mysql restart

        service mysql restart || {
            echo "Failed to restart MySQL. Exiting..."
            exit 1
        }
        break # Exit the loop after installing MySQL and setting the password
    fi
done

echo "Configuring MySQL..."
desired_sql_mode="sql_mode ="
desired_innodb_strict_mode="innodb_strict_mode = 0"
desired_charset="character-set-server=utf8mb4"
desired_collation="collation-server=utf8mb4_general_ci"
desired_auth_plugin="default_authentication_plugin=mysql_native_password"
config_file="/etc/mysql/mysql.conf.d/mysqld.cnf"

cp ${config_file} ${config_file}.bak

awk -v dsm="${desired_sql_mode}" -v dism="${desired_innodb_strict_mode}" \
    -v dcharset="${desired_charset}" -v dcollation="${desired_collation}" \
    -v dauth="${desired_auth_plugin}" \
    'BEGIN { sql_mode_added=0; innodb_strict_mode_added=0; charset_added=0; collation_added=0; auth_plugin_added=0; }
                /default_authentication_plugin[[:space:]]*=/ {
                    if ($0 ~ dauth) {auth_plugin_added=1;}
                    else {print ";" $0;}
                    next;
                }
                /sql_mode[[:space:]]*=/ {
                    if ($0 ~ dsm) {sql_mode_added=1;}
                    else {print ";" $0;}
                    next;
                }
                /innodb_strict_mode[[:space:]]*=/ {
                    if ($0 ~ dism) {innodb_strict_mode_added=1;}
                    else {print ";" $0;}
                    next;
                }
                /character-set-server[[:space:]]*=/ {
                    if ($0 ~ dcharset) {charset_added=1;}
                    else {print ";" $0;}
                    next;
                }
                /collation-server[[:space:]]*=/ {
                    if ($0 ~ dcollation) {collation_added=1;}
                    else {print ";" $0;}
                    next;
                }
                /skip-external-locking|mysqlx-bind-address/ {
                    print;
                    if (sql_mode_added == 0) {print dsm; sql_mode_added=1;}
                    if (innodb_strict_mode_added == 0) {print dism; innodb_strict_mode_added=1;}
                    if (charset_added == 0) {print dcharset; charset_added=1;}
                    if (collation_added == 0) {print dcollation; collation_added=1;}
                    next;
                }
                { print; }' ${config_file} >tmpfile && mv tmpfile ${config_file}

service mysql restart || {
    mv ${config_file}.bak ${config_file}
    echo "Failed to restart MySQL. Exiting..."
    exit 1
}
# Accept optional PHP version parameter, default to 8.2
PHP_VERSION=${1:-8.2}
# PHP Setup
echo "Installing and configuring PHP $PHP_VERSION..."

wget https://raw.githubusercontent.com/deforay/utility-scripts/master/php/switch-php -O /usr/local/bin/switch-php
chmod u+x /usr/local/bin/switch-php

switch-php $PHP_VERSION

# phpMyAdmin Setup
if [ ! -d "/var/www/phpmyadmin" ]; then
    echo "Downloading and setting up phpMyAdmin..."

    # Create the directory if it does not exist
    mkdir -p /var/www/phpmyadmin

    # Download the ZIP file
    # Replace the URL with the latest ZIP file URL from the phpMyAdmin website
    wget -q --show-progress --progress=dot:giga https://www.phpmyadmin.net/downloads/phpMyAdmin-latest-all-languages.zip

    # Extract directly into the /var/www/phpmyadmin directory
    unzip -q phpMyAdmin-latest-all-languages.zip -d /var/www/phpmyadmin || {
        echo "Extraction failed"
        exit 1
    }

    # Clean up the downloaded ZIP file
    rm phpMyAdmin-latest-all-languages.zip

    # The unzip command extracts the files into a subdirectory. We need to move them up one level.
    PHPMYADMIN_DIR=$(ls /var/www/phpmyadmin)
    mv /var/www/phpmyadmin/$PHPMYADMIN_DIR/* /var/www/phpmyadmin/
    mv /var/www/phpmyadmin/$PHPMYADMIN_DIR/.[!.]* /var/www/phpmyadmin/ 2>/dev/null
    rmdir /var/www/phpmyadmin/$PHPMYADMIN_DIR

    echo "Configuring Apache for phpMyAdmin..."
    desired_alias="Alias /phpmyadmin /var/www/phpmyadmin"
    config_file="/etc/apache2/sites-available/000-default.conf"

    # Check if the desired alias already exists
    if ! grep -q "$desired_alias" ${config_file}; then
        awk -v da="$desired_alias" \
            'BEGIN {added=0; alias_added=0}
        /Alias \/phpmyadmin[[:space:]]/ {
            if ($0 !~ da) {print ";" $0} else {alias_added=1; print $0}
            next;
        }
        /ServerAdmin|DocumentRoot/ {
            print;
            if (added == 0 && alias_added == 0) {
                print da;
                added=1;
            }
            next;
        }
        { print }' ${config_file} >tmpfile && mv tmpfile ${config_file}
    fi

    service apache2 restart
fi

service apache2 restart || {
    echo "Failed to restart Apache2. Exiting..."
    log_action "Failed to restart Apache2. Exiting..."
    exit 1
}

log_action "LAMP Setup complete."
echo "LAMP Setup complete."
