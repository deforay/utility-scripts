#!/bin/bash

# To use this script:
# sudo -s
# cd /tmp
# wget -O ./lamp-setup.sh https://raw.githubusercontent.com/deforay/utility-scripts/master/lamp/lamp-setup.sh
# chmod +x ./lamp-setup.sh
# ./lamp-setup.sh [PHP_VERSION]
# ./lamp-setup.sh [PHP_VERSION] -f
# ./lamp-setup.sh [PHP_VERSION] --force

#=============================================================================
# UTILITY FUNCTIONS
#=============================================================================

# Define a unified print function that colors the entire message
print() {
    local type=$1
    local message=$2

    case $type in
    error)
        echo -e "\033[0;31mError: $message\033[0m"
        ;;
    success)
        echo -e "\033[0;32mSuccess: $message\033[0m"
        ;;
    warning)
        echo -e "\033[0;33mWarning: $message\033[0m"
        ;;
    info)
        echo -e "\033[0;36mInfo: $message\033[0m"
        ;;
    debug)
        echo -e "\033[1;36mDebug: $message\033[0m"
        ;;
    header)
        echo -e "\033[1;36m==== $message ====\033[0m"
        ;;
    *)
        echo "$message"
        ;;
    esac
}

ask_yes_no() {
    local timeout=15
    local default=${2:-"no"}
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

# Function to log messages
log_action() {
    local message=$1
    echo "$(date +'%Y-%m-%d %H:%M:%S') - $message" >>~/logsetup.log
}

error_handling() {
    local last_cmd=$1
    local last_line=$2
    local last_error=$3
    print error "Error on or near line ${last_line}; command executed was '${last_cmd}' which exited with status ${last_error}"
    log_action "Error on or near line ${last_line}; command executed was '${last_cmd}' which exited with status ${last_error}"

    if [ "$last_error" -eq 1 ]; then
        print error "This error is critical, exiting..."
        exit 1
    else
        print info "This error is not critical, continuing..."
    fi
}

#=============================================================================
# VALIDATION FUNCTIONS
#=============================================================================

check_root_privileges() {
    if [ "$EUID" -ne 0 ]; then
        print error "Need admin privileges for this script. Run sudo -s before running this script or run this script with sudo"
        exit 1
    fi
}

is_valid_ubuntu() {
    if ! lsb_release -d | grep -q "LTS"; then
        print error "This script only runs on Ubuntu LTS releases."
        return 1
    fi

    local min_version="22.04"
    local current_version=$(lsb_release -rs)

    if [[ "$(printf '%s\n' "$min_version" "$current_version" | sort -V | head -n1)" != "$min_version" ]]; then
        print error "This script requires Ubuntu ${min_version} LTS or newer."
        return 1
    fi

    return 0
}

# Pick the default PHP major.minor for the running OS when the caller didn't
# pass a version. Ubuntu <=24.04 carries (and ondrej's PPA still builds) PHP
# 8.4; Ubuntu 26.04 dropped 8.4 and ships 8.5, so default to 8.5 there. Any
# explicit version passed on the command line always wins over this.
default_php_version_for_os() {
    local ubuntu_version
    ubuntu_version=$(lsb_release -rs 2>/dev/null || echo "")

    # Can't detect the release -> stick with the long-standing baseline.
    if [[ -z "$ubuntu_version" ]]; then
        echo "8.4"
        return
    fi

    # 26.04 and newer no longer provide PHP 8.4; use 8.5.
    if [[ "$(printf '%s\n' "26.04" "$ubuntu_version" | sort -V | head -n1)" == "26.04" ]]; then
        echo "8.5"
    else
        echo "8.4"
    fi
}

check_dependencies() {
    for cmd in "apt-get"; do
        if ! command -v $cmd &>/dev/null; then
            print error "$cmd is not installed. Exiting..."
            exit 1
        fi
    done
}

#=============================================================================
# SYSTEM SETUP FUNCTIONS
#=============================================================================

# apt-get update, but an unreadable third-party source file (e.g. a malformed
# teamviewer.list, which makes apt refuse to run at all) no longer stops setup:
# that file is moved to /root/apt-sources-disabled and the update retried.
# Only files apt itself names as unparseable are moved, and never the distro's
# own sources or a Launchpad PPA (setup needs ondrej). Every other apt error
# (missing Release file, conflicting options, unsigned repo) is reported with
# the files to look at, because guessing which entry is broken could disable a
# healthy or required source.
APT_DISABLED_DIR=/root/apt-sources-disabled

apt_update_or_heal() {
    local out file attempt
    local -a bad
    for attempt in 1 2 3 4; do
        if out=$(apt-get update 2>&1); then
            return 0
        fi
        ((attempt < 4)) || break
        mapfile -t bad < <(grep -E '^E: (Malformed|Type .* is not known|Unable to parse)' <<<"$out" |
            grep -oE '/etc/apt/sources\.list\.d/[^ :()]+' | sort -u || true)
        local -a movable=()
        for file in "${bad[@]}"; do
            [[ -f "$file" ]] || continue
            if [[ "$file" =~ /(ubuntu|debian)\.sources$ ]] || grep -qE 'launchpad(content)?\.net|ppa\.launchpad' "$file"; then
                continue
            fi
            movable+=("$file")
        done
        if ((${#movable[@]} == 0)); then
            # Download failures alone (flaky link, mirror hiccup) are not a broken
            # source: apt keeps the previous lists, so warn and carry on.
            if ! grep -E '^E:' <<<"$out" | grep -qvE 'Failed to fetch|Some index files failed to download'; then
                print warning "Some package lists could not be downloaded (network?); continuing with cached lists."
                grep -E '^E: Failed to fetch' <<<"$out" | head -n 5 || true
                log_action "apt-get update: download failures ignored"
                return 0
            fi
            break
        fi
        mkdir -p "$APT_DISABLED_DIR"
        for file in "${movable[@]}"; do
            print warning "Disabling unreadable apt source: $file (moved to $APT_DISABLED_DIR)"
            log_action "Disabled unreadable apt source $file"
            mv -f "$file" "$APT_DISABLED_DIR/"
        done
    done

    print error "Updating the package list failed, and the cause could not be fixed automatically:"
    grep -E '^(E|W):' <<<"$out" | head -n 10 || true
    # Errors like "Conflicting values ... regarding source URL" name no file; list
    # the files that mention each repository so the operator knows where to look.
    local url files
    while IFS= read -r url; do
        files=$(grep -lF -- "$url" /etc/apt/sources.list /etc/apt/sources.list.d/* 2>/dev/null | tr '\n' ' ')
        [[ -n "$files" ]] && print info "Files mentioning ${url}: ${files}"
    done < <(grep -E '^E:' <<<"$out" | grep -oE 'https?://[^ /]+(/[^ ]*)?' | sed -E 's#/(dists|InRelease|Release)(/.*)?$##; s#/$##' | sort -u || true)
    print info "Fix or remove the source file mentioned above (in /etc/apt/sources.list or /etc/apt/sources.list.d/),"
    print info "for example: sudo mv /etc/apt/sources.list.d/<file> /root/  then run setup again."
    log_action "apt-get update failed: $(grep -E '^E:' <<<"$out" | head -n 1 || true)"
    exit 1
}

setup_system() {
    print header "Setting up system basics..."

    # Update Ubuntu Packages (always safe to run)
    print info "Updating Ubuntu packages..."
    apt_update_or_heal
    apt-get upgrade -y

    # Configure any packages that were not fully installed (always safe)
    print info "Configuring any partially installed packages..."
    dpkg --configure -a

    # Clean up (always safe)
    apt-get autoremove -y

    # Check if basic packages are already installed
    print info "Checking and installing basic packages..."
    local packages_to_install=()
    local basic_packages=(build-essential software-properties-common gnupg apt-transport-https ca-certificates lsb-release wget vim zip unzip curl acl snapd rsync git gdebi net-tools sed mawk magic-wormhole openssh-server libsodium-dev mosh aria2 wget lsb-release bc pigz gpg certbot python3-certbot-apache)

    for package in "${basic_packages[@]}"; do
        if ! dpkg -l | grep -q "^ii  $package "; then
            packages_to_install+=("$package")
        fi
    done

    if [ ${#packages_to_install[@]} -gt 0 ]; then
        print info "Installing missing packages: ${packages_to_install[*]}"
        apt-get install -y "${packages_to_install[@]}"
    else
        print info "All basic packages are already installed"
    fi

    # Add ondrej/apache2 only if Launchpad publishes the current codename.
    # Cross-codename installs break due to ABI mismatches (libxml2 t64 etc.),
    # so on unsupported Ubuntu releases we stick with the distro apache2.
    local apache_ppa="ondrej/apache2"
    local codename
    codename=$(lsb_release -cs)
    local apache_list="/etc/apt/sources.list.d/ondrej-ubuntu-apache2-${codename}.list"
    local apache_sources="/etc/apt/sources.list.d/ondrej-ubuntu-apache2-${codename}.sources"

    if [[ -f "${apache_list}.disabled" || -f "${apache_sources}.disabled" ]]; then
        print info "${apache_ppa} previously disabled on '${codename}'; using distro apache2."
    elif grep -q "ondrej/apache2" /etc/apt/sources.list /etc/apt/sources.list.d/* 2>/dev/null; then
        print info "${apache_ppa} PPA already added"
        disable_apache2_ppa_if_unsupported "$codename" "$apache_list" "$apache_sources"
    elif ! ppa_has_codename "$apache_ppa" "$codename"; then
        print warning "${apache_ppa} has no build for '${codename}'; skipping PPA, using distro apache2."
    else
        print info "Adding ${apache_ppa} PPA..."
        add-apt-repository ppa:ondrej/apache2 -y
        apt-get update
    fi
}

ppa_has_codename() {
    local ppa_path="$1" cn="$2"
    curl -fsSLI -o /dev/null --max-time 10 \
        "https://ppa.launchpadcontent.net/${ppa_path}/ubuntu/dists/${cn}/Release" 2>/dev/null
}

# If ondrej/apache2 doesn't publish $codename, disable its source files so apt
# falls back to the distro archive. Heals a previous noble-pin by restoring
# the codename inside the file to match its filename suffix before disabling.
disable_apache2_ppa_if_unsupported() {
    local codename="$1" apache_list="$2" apache_sources="$3"

    if ppa_has_codename "ondrej/apache2" "$codename"; then
        [[ -f "$apache_sources" ]] && sed -i -E "s/^(Suites:[[:space:]]*).*/\1${codename}/" "$apache_sources" 2>/dev/null || true
        [[ -f "$apache_list" ]]    && sed -i -E "s|(ubuntu[[:space:]]+)[a-zA-Z]+([[:space:]]+main)|\1${codename}\2|" "$apache_list" 2>/dev/null || true
        return 0
    fi

    print warning "ondrej/apache2 has no build for '${codename}'; disabling PPA, falling back to distro apache2."
    [[ -f "$apache_sources" ]] && sed -i -E "s/^(Suites:[[:space:]]*).*/\1${codename}/" "$apache_sources" 2>/dev/null || true
    [[ -f "$apache_list" ]]    && sed -i -E "s|(ubuntu[[:space:]]+)[a-zA-Z]+([[:space:]]+main)|\1${codename}\2|" "$apache_list" 2>/dev/null || true
    [[ -f "$apache_sources" ]] && mv "$apache_sources" "${apache_sources}.disabled"
    [[ -f "$apache_list" ]]    && mv "$apache_list" "${apache_list}.disabled"
    apt-get update || true
}

setup_locale() {
    print header "Setting up locale..."

    # Check if locale is already generated
    if locale -a | grep -q "en_US.utf8"; then
        print info "en_US.UTF-8 locale already generated"
    else
        print info "Generating en_US.UTF-8 locale..."
        locale-gen en_US en_US.UTF-8
    fi

    # Set environment variables (always safe to do)
    export LANG=en_US.UTF-8
    export LC_ALL=en_US.UTF-8

    # Update locale configuration (idempotent)
    update-locale LANG=en_US.UTF-8 LC_ALL=en_US.UTF-8
}

setup_ssh() {
    print header "Setting up SSH..."

    if ! systemctl is-enabled ssh >/dev/null 2>&1; then
        print info "Enabling SSH service..."
        systemctl enable ssh
    else
        print info "SSH service is already enabled."
    fi

    if ! systemctl is-active ssh >/dev/null 2>&1; then
        print info "Starting SSH service..."
        systemctl start ssh
    else
        print info "SSH service is already running."
    fi
}

#=============================================================================
# APACHE SETUP FUNCTIONS
#=============================================================================

# True when at least one package matching the dpkg pattern is unpacked and
# configured. Reads the state field alone so held packages ("hold ok installed")
# still count, and half-installed/removed ("rc") ones do not.
pkg_installed() {
    dpkg-query -W -f='${db:Status-Eflag} ${db:Status-Status}\n' "$1" 2>/dev/null | grep -qx "ok installed"
}

# Installed package names matching the dpkg pattern, one per line.
installed_pkgs() {
    dpkg-query -W -f='${db:Status-Status} ${Package}\n' "$@" 2>/dev/null | awk '$1 == "installed" {print $2}'
}

# True when the apache2 package is installed AND its config tree is intact.
# `command -v apache2` is not enough: the binary ships in apache2-bin, so after
# `apt remove/purge apache2` (or a deleted /etc/apache2) the binary is still
# there while apache2.conf, a2enmod's mods-available/ and the unit are gone.
apache_config_intact() {
    pkg_installed apache2 &&
        [[ -f /etc/apache2/apache2.conf ]] &&
        [[ -f /etc/apache2/envvars ]] &&
        apache_required_modules_present
}

# mpm_prefork first: until an MPM is loaded every apache2ctl call errors.
APACHE_REQUIRED_MODULES=(mpm_prefork rewrite headers deflate env)

apache_required_modules_present() {
    local module
    for module in "${APACHE_REQUIRED_MODULES[@]}"; do
        [[ -f "/etc/apache2/mods-available/${module}.load" ]] || return 1
    done
}

# Explain why Apache will not start, in terms an operator can act on, then exit.
apache_start_failed() {
    print error "Apache could not be started."
    log_action "Apache could not be started."

    if ! apache2ctl configtest >/dev/null 2>&1; then
        print error "Apache configuration test failed:"
        apache2ctl configtest 2>&1 | tail -n 10
    fi

    local port_owner
    port_owner=$(ss -ltnp 2>/dev/null | awk '$4 ~ /:(80|443)$/ && $0 !~ /apache2/ {print $4, $6}' | head -n 3)
    if [[ -n "$port_owner" ]]; then
        print error "Port 80/443 is already used by another program:"
        echo "$port_owner"
        print info "Stop or remove that program (for example: systemctl disable --now nginx), then run setup again."
    fi

    print info "Last Apache log lines:"
    journalctl -u apache2 --no-pager -n 15 2>/dev/null || true
    print info "If Apache was removed or its /etc/apache2 folder deleted, repair it with:"
    print info "  sudo apt-get purge -y apache2 apache2-bin apache2-data apache2-utils && sudo apt-get install -y apache2"
    print info "then run setup again."
    exit 1
}

install_apache() {
    print header "Setting up Apache..."

    if apache_config_intact; then
        print info "Apache is already installed. Checking configuration..."
    elif command -v apache2 &>/dev/null || [[ -d /etc/apache2 ]]; then
        # Binary present but package/config missing: restore the deleted
        # conffiles instead of trusting the half-installed state.
        print warning "Apache is partly installed (configuration files missing). Repairing..."
        log_action "Apache partly installed; reinstalling with --force-confmiss"
        apt-get install -y --reinstall -o Dpkg::Options::="--force-confmiss" \
            apache2 apache2-bin apache2-data apache2-utils || true
    else
        print info "Installing Apache..."
        apt-get install -y apache2
    fi

    if ! apache_config_intact; then
        print error "Apache is installed but its configuration files are missing, and automatic repair did not restore them."
        print info "Repair it with:"
        print info "  sudo apt-get purge -y apache2 apache2-bin apache2-data apache2-utils && sudo apt-get install -y apache2"
        print info "then run setup again."
        log_action "Apache config still missing after repair attempt"
        exit 1
    fi

    # Leftovers from removed packages stop Apache from starting: links in
    # *-enabled/ whose target is gone, and enabled modules whose .so was removed
    # (typically libphpX.Y.so after that PHP version was uninstalled).
    local link mod_load so
    for link in /etc/apache2/{mods,conf,sites}-enabled/*; do
        if [[ -L "$link" && ! -e "$link" ]]; then
            print warning "Removing broken Apache link: $link"
            rm -f "$link"
        fi
    done
    # Only when the config is already broken, and only for plain absolute paths
    # (quotes stripped); anything else is left to Apache rather than guessed at.
    if ! apache2ctl configtest >/dev/null 2>&1; then
    for mod_load in /etc/apache2/mods-enabled/*.load; do
        [[ -e "$mod_load" ]] || continue
        so=$(awk '$1 == "LoadModule" {gsub(/"/, "", $3); print $3; exit}' "$mod_load")
        [[ "$so" == /* && "$so" != *'$'* ]] || continue
        if [[ ! -f "$so" ]]; then
            print warning "Disabling Apache module $(basename "$mod_load" .load): $so is missing"
            a2dismod -f "$(basename "$mod_load" .load)" || true
        fi
    done
    fi

    # Check and configure modules (idempotent)
    if apache2ctl -M 2>/dev/null | grep -q "mpm_event_module"; then
        print info "Disabling mpm_event module..."
        a2dismod mpm_event
    fi

    # Enable required modules if not already enabled
    local module
    for module in "${APACHE_REQUIRED_MODULES[@]}"; do
        if ! apache2ctl -M 2>/dev/null | grep -q "${module}_module"; then
            print info "Enabling Apache module: $module"
            a2enmod $module
        else
            print info "Apache module $module already enabled"
        fi
    done

    # Restart only if needed
    if ! systemctl is-active apache2 >/dev/null 2>&1; then
        print info "Starting Apache service..."
        service apache2 start || apache_start_failed
    else
        print info "Restarting Apache to apply any changes..."
        service apache2 restart || apache_start_failed
    fi
}

setup_brotli() {
    print info "Setting up Brotli compression..."

    if ! apache2ctl -M | grep -q 'brotli_module'; then
        if dpkg -l | grep -q '^ii  libapache2-mod-brotli '; then
            print info "libapache2-mod-brotli already installed"
        else
            print info "Installing libapache2-mod-brotli..."
            log_action "Installing libapache2-mod-brotli..."
            # Refresh package lists and confirm availability before install
            apt-get update -y || true
            if ! apt-cache show libapache2-mod-brotli >/dev/null 2>&1; then
                print warning "libapache2-mod-brotli not available in current repos. Skipping Brotli setup."
                log_action "libapache2-mod-brotli not available in current repos. Skipping Brotli setup."
                return
            fi

            apt-get install -y libapache2-mod-brotli || {
                print warning "Failed to install libapache2-mod-brotli. Continuing without Brotli support..."
                log_action "Failed to install libapache2-mod-brotli. Continuing without Brotli support..."
                return
            }
        fi

        print info "Enabling Brotli module..."
        a2enmod brotli
        service apache2 restart || {
            print error "Failed to restart Apache after enabling Brotli. Exiting..."
            exit 1
        }
    else
        print info "Brotli module is already installed and enabled."
        log_action "Brotli module is already installed and enabled."
    fi
}

configure_basic_performance() {
    print header "Configuring basic Apache performance..."

    # Check and enable modules only if not already enabled
    for module in deflate expires headers; do
        if ! apache2ctl -M | grep -q "${module}_module"; then
            print info "Enabling Apache module: $module"
            a2enmod $module
        else
            print info "Apache module $module already enabled"
        fi
    done

    # Check if performance config already exists and is enabled
    config_file="/etc/apache2/conf-available/basic-performance.conf"

    if [ ! -f "$config_file" ]; then
        print info "Creating basic performance configuration..."
        cat >"$config_file" <<'EOF'
# Enable compression
<IfModule mod_deflate.c>
    AddOutputFilterByType DEFLATE text/html text/plain text/xml text/css text/javascript application/javascript
</IfModule>

# Basic security headers
<IfModule mod_headers.c>
    Header always set X-Content-Type-Options nosniff
    Header always set X-Frame-Options SAMEORIGIN
</IfModule>
EOF
    else
        print info "Basic performance configuration already exists"
    fi

    # Check if config is enabled
    if [ ! -f "/etc/apache2/conf-enabled/basic-performance.conf" ]; then
        print info "Enabling basic performance configuration..."
        a2enconf basic-performance
    else
        print info "Basic performance configuration already enabled"
    fi

    print success "Basic Apache performance configured"
}

setup_web_permissions() {
    print info "Setting up web directory permissions..."

    # Check if directory exists and has proper permissions
    if [ -d "/var/www" ]; then
        # Only set permissions if they're not already correct
        if ! getfacl /var/www 2>/dev/null | grep -q "user:www-data:rwx"; then
            print info "Setting ACL permissions for /var/www..."
            setfacl -R -m u:$USER:rwx,u:www-data:rwx /var/www
        else
            print info "Web directory permissions already configured"
        fi
    else
        print warning "/var/www directory does not exist yet"
    fi
}

#=============================================================================
# MYSQL SETUP FUNCTIONS
#=============================================================================

# MariaDB is not a drop-in for the apps this sets up (MySQL 8 collations such as
# utf8mb4_0900_ai_ci and the ->> JSON operator). Replacing it automatically could
# discard a lab's data, so stop and say what to do instead.
refuse_mariadb() {
    pkg_installed 'mariadb-server*' || return 0
    print error "MariaDB is installed. This setup needs MySQL 8; MariaDB is not compatible."
    print info "If the MariaDB databases are needed, back them up first:"
    print info "  sudo mysqldump --all-databases > ~/mariadb-backup.sql"
    print info "Then remove MariaDB (this deletes its databases):"
    print info "  sudo apt-get purge -y 'mariadb-*' && sudo rm -rf /var/lib/mysql /etc/mysql"
    print info "and run setup again."
    log_action "MariaDB installed; setup stopped (MySQL 8 required)"
    exit 1
}

# Up = accepting connections. mysqladmin ping reports "alive" even when access
# is denied, which is exactly what this needs and why it must never be used to
# test a password.
mysql_alive() {
    mysqladmin --no-defaults ping &>/dev/null
}

# Does this root password work? Runs a statement; --no-defaults (first) so a
# stale /root/.my.cnf cannot answer instead. Root on auth_socket accepts OS root
# whatever the password, and enforce_mysql_root_password then sets it.
mysql_root_password_works() {
    [[ -n "$1" ]] && mysql --no-defaults -u root -p"$1" -e 'SELECT 1' &>/dev/null
}

# Installed MySQL must be running before credentials mean anything. If it will
# not start, prompting for passwords would loop forever, so explain and stop.
ensure_mysql_running() {
    mysql_alive && return 0
    service mysql start &>/dev/null || systemctl start mysql &>/dev/null || true
    local i
    for i in $(seq 1 20); do
        mysql_alive && return 0
        sleep 1
    done
    print error "MySQL is installed but is not running and could not be started."
    systemctl status mysql --no-pager 2>/dev/null | head -n 8 || true
    print info "Last MySQL log lines:"
    journalctl -u mysql --no-pager -n 20 2>/dev/null || tail -n 20 /var/log/mysql/error.log 2>/dev/null || true
    print info "If this PC has no MySQL data to keep, reinstall MySQL from scratch (deletes its databases):"
    print info "  sudo systemctl stop mysql; sudo apt-get purge -y 'mysql-server*' mysql-common"
    print info "  sudo rm -rf /var/lib/mysql /etc/mysql && sudo apt-get install -y mysql-server"
    print info "then run setup again."
    log_action "MySQL installed but not running; setup stopped"
    exit 1
}

mysql_password_reset_help() {
    print error "The MySQL root password was not accepted after 3 attempts."
    print info "An earlier install may have recorded it (e.g. /var/www/intelis/configs/config.production.php)."
    print info "If root can still log in over the socket, set a new one:"
    print info "  sudo mysql --no-defaults -e \"ALTER USER 'root'@'localhost' IDENTIFIED WITH caching_sha2_password BY 'NewPassword'; FLUSH PRIVILEGES;\""
    print info "If that is refused and this PC has no MySQL data to keep, reinstall MySQL (deletes its databases):"
    print info "  sudo systemctl stop mysql; sudo apt-get purge -y 'mysql-server*' mysql-common"
    print info "  sudo rm -rf /var/lib/mysql /etc/mysql && sudo apt-get install -y mysql-server"
    print info "then run setup again."
    log_action "MySQL root password rejected 3 times; setup stopped"
    exit 1
}

get_mysql_password() {
    print header "Setting up MySQL credentials..."
    refuse_mariadb

    local server_present=false
    if mysql_server_intact; then
        server_present=true
        ensure_mysql_running
    fi

    mysql_root_password=""
    mysql_root_password_confirm=""

    # Accept a password supplied by the caller (e.g. intelis setup.sh, which has
    # already collected it) via the MYSQL_ROOT_PASSWORD env var, so we don't
    # prompt a second time. If MySQL is already running and the supplied password
    # fails to authenticate, fall through to the normal prompt rather than
    # trusting it blindly; pre-install (no mysqld yet) we accept it and the
    # install step sets root to this value.
    if [ -n "${MYSQL_ROOT_PASSWORD:-}" ]; then
        mysql_root_password="${MYSQL_ROOT_PASSWORD}"
        if $server_present && ! mysql_root_password_works "${mysql_root_password}"; then
            print warning "The MySQL root password entered earlier does not work on the MySQL already installed here; prompting."
            mysql_root_password=""
        else
            print info "Using MySQL root password supplied by the caller."
            print info "Storing MySQL root password securely..."
            cat <<EOF >~/.my.cnf
[client]
user=root
password=${mysql_root_password}
host=localhost
EOF
            chmod 600 ~/.my.cnf
            return
        fi
    fi

    # Reuse existing password from ~/.my.cnf when valid
    if [ -f ~/.my.cnf ]; then
        mysql_root_password=$(awk -F= '/^password[[:space:]]*=/{gsub(/^[ \t]+|[ \t]+$/,"",$2); print $2; exit}' ~/.my.cnf)
        if [ -n "$mysql_root_password" ]; then
            print info "Found existing MySQL credentials in ~/.my.cnf; verifying..."
            if ! $server_present || mysql_root_password_works "${mysql_root_password}"; then
                print success "Reusing MySQL root password from ~/.my.cnf"
                return
            else
                print warning "Existing ~/.my.cnf password did not work; prompting for a new one."
                mysql_root_password=""
            fi
        fi
    fi

    local attempts=0
    while :; do
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

        print info "Storing MySQL root password securely..."
        cat <<EOF >~/.my.cnf
[client]
user=root
password=${mysql_root_password}
host=localhost
EOF
        chmod 600 ~/.my.cnf

        # Verify MySQL password
        if $server_present; then
            print info "MySQL is already installed. Verifying password..."
            if mysql_root_password_works "${mysql_root_password}"; then
                print success "Password verified."
                break
            fi
            attempts=$((attempts + 1))
            ((attempts >= 3)) && mysql_password_reset_help
            print warning "That is not the root password of the MySQL installed here. Please try again."
            mysql_root_password=""
            mysql_root_password_confirm=""
        else
            break # MySQL not installed, continue to installation
        fi
    done
}

# Wait until the local mysqld accepts connections (socket, as OS root).
wait_for_mysql() {
    local i
    for i in $(seq 1 30); do
        if mysqladmin ping &>/dev/null; then
            return 0
        fi
        sleep 1
    done
    return 1
}

# Idempotently switch root@localhost to password authentication and VERIFY it
# over TCP — the way the application connects.
#
# Fresh MySQL 8 ships root on the auth_socket plugin, which authenticates by the
# OS peer user over the unix socket. That makes every check run here as OS root
# (mysqladmin ping, mysql -u root) succeed even though the app — connecting with
# a password as www-data — gets "Access denied for user 'root'@'localhost'". So
# we run the ALTER as OS root via the socket (works whether root is still
# auth_socket or already password-auth via ~/.my.cnf), pin it to
# mysql_native_password (what PHP/LIS uses), and then confirm with a real TCP
# password login, which auth_socket cannot satisfy.
enforce_mysql_root_password() {
    print info "Ensuring MySQL root uses password authentication..."
    if ! wait_for_mysql; then
        print error "MySQL is not accepting connections; cannot configure root auth. Exiting..."
        exit 1
    fi

    # Pick the auth plugin by server flavor:
    #   - MySQL 8.x: caching_sha2_password — the modern default and future-proof
    #     (mysql_native_password is deprecated and disabled by default on 8.4+).
    #     PHP 7.4+/mysqlnd speaks it over both socket and TCP.
    #   - MariaDB: has no caching_sha2_password, so use mysql_native_password.
    # Within MySQL we fall back to native_password if caching_sha2 can't be set.
    # (Longer term the app should use a dedicated non-root user and leave root on
    # auth_socket for local admin.)
    local plugins="caching_sha2_password mysql_native_password"
    if mysql --version 2>/dev/null | grep -qi mariadb; then
        plugins="mysql_native_password"
    fi

    # Verify as a NON-root OS user over the socket — exactly how the app (www-data)
    # connects. The socket is a secure transport, so caching_sha2 needs no
    # public-key exchange, native_password works too, and auth_socket correctly
    # FAILS this check (it would only pass for OS user root). This is what every
    # OS-root/socket check in this script missed.
    local verify_user="nobody"
    local run_as_verify="runuser -u ${verify_user} --"
    command -v runuser >/dev/null 2>&1 || run_as_verify="sudo -u ${verify_user}"

    local plugin attempt
    for plugin in $plugins; do
        for attempt in 1 2 3; do
            if mysql -e "ALTER USER 'root'@'localhost' IDENTIFIED WITH ${plugin} BY '${mysql_root_password}'; FLUSH PRIVILEGES;" &>/dev/null \
                && $run_as_verify mysql -u root -p"${mysql_root_password}" -e "SELECT 1;" &>/dev/null; then
                print success "MySQL root password authentication verified (${plugin}, non-root over socket)."
                return 0
            fi
            sleep 2
        done
        print warning "Could not set root via ${plugin}; trying the next option..."
    done

    print error "Could not configure MySQL root for password authentication."
    print error "The application connects with a password and will fail until this is resolved."
    exit 1
}

# True when the MySQL SERVER is installed with its config. `command -v mysql`
# only proves the client exists, which survives removing the server. The unit
# file belongs to the server package (mysql-server-8.0 / -8.4), so it also
# catches a removed server whose core package and conffiles were left behind.
# 'mysql-server-*' matches both versioned core names and 26.04's unversioned
# mysql-server-core; 'mysql-community-server*' is Oracle's own APT repo.
# MariaDB never reaches here: refuse_mariadb stops setup first.
mysql_server_intact() {
    command -v mysqld &>/dev/null &&
        [[ -f /etc/mysql/my.cnf ]] &&
        [[ -f /etc/mysql/mysql.conf.d/mysqld.cnf ]] &&
        [[ -d /etc/mysql/conf.d ]] &&
        [[ -f /lib/systemd/system/mysql.service || -f /usr/lib/systemd/system/mysql.service ]] &&
        { pkg_installed 'mysql-server-*' || pkg_installed 'mysql-community-server*'; }
}

install_mysql() {
    if mysql_server_intact; then
        :
    elif command -v mysqld &>/dev/null || [[ -d /etc/mysql ]]; then
        print warning "MySQL server is partly installed (files missing). Repairing..."
        log_action "MySQL partly installed; reinstalling with --force-confmiss"
        local mysql_pkgs
        mapfile -t mysql_pkgs < <(installed_pkgs 'mysql-server-*' 'mysql-community-server*' 'mysql-common')
        apt-get install -y --reinstall -o Dpkg::Options::="--force-confmiss" \
            mysql-server "${mysql_pkgs[@]}" || true
    else
        print header "Installing MySQL..."
        apt-get install -y mysql-server
    fi
    if ! mysql_server_intact; then
        print error "MySQL server is not installed correctly and automatic repair did not fix it."
        print info "Repair it with:"
        print info "  sudo apt-get install -y --reinstall mysql-server"
        print info "then run setup again."
        log_action "MySQL server still missing after install/repair attempt"
        exit 1
    fi
    service mysql start &>/dev/null || systemctl start mysql &>/dev/null || true

    # Always (re-)assert password auth, even when MySQL was already installed, so
    # a prior run that left root on auth_socket self-heals on the next run.
    enforce_mysql_root_password
}

configure_mysql() {
    print header "Configuring MySQL..."

    desired_sql_mode="sql_mode ="
    desired_innodb_strict_mode="innodb_strict_mode = 0"
    desired_charset="character-set-server=utf8mb4"
    desired_collation="collation-server=utf8mb4_general_ci"
    config_file="/etc/mysql/mysql.conf.d/mysqld.cnf"

    # Check if config already has all our desired settings
    sql_mode_exists=$(grep -c "^${desired_sql_mode}" ${config_file} || true)
    innodb_mode_exists=$(grep -c "^${desired_innodb_strict_mode}" ${config_file} || true)
    charset_exists=$(grep -c "^${desired_charset}" ${config_file} || true)
    collation_exists=$(grep -c "^${desired_collation}" ${config_file} || true)
    legacy_auth_plugin=$(grep -c "^default_authentication_plugin" ${config_file} || true)

    if [ $sql_mode_exists -gt 0 ] && [ $innodb_mode_exists -gt 0 ] && [ $charset_exists -gt 0 ] && [ $collation_exists -gt 0 ] && [ $legacy_auth_plugin -eq 0 ]; then
        print info "MySQL already has all the required configurations. Skipping configuration."
    else
        print info "Some MySQL configurations need to be updated."
        cp ${config_file} ${config_file}.bak

        awk -v dsm="${desired_sql_mode}" -v dism="${desired_innodb_strict_mode}" \
            -v dcharset="${desired_charset}" -v dcollation="${desired_collation}" \
            'BEGIN { sql_mode_added=0; innodb_strict_mode_added=0; charset_added=0; collation_added=0; }
                        /default_authentication_plugin[[:space:]]*=/ {
                            # Removed in MySQL 8.4; comment out any pre-existing entry.
                            print ";" $0;
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
            print error "Failed to restart MySQL. Exiting..."
            exit 1
        }
    fi
}

secure_mysql() {
    print header "Securing MySQL installation..."

    # Test if we can connect (validates our setup is working)
    if ! mysql -u root -p"${mysql_root_password}" -e "SELECT 1;" &>/dev/null; then
        print error "Cannot connect to MySQL. Skipping security setup."
        return 1
    fi

    # These operations are idempotent - MySQL ignores if already done
    mysql -u root -p"${mysql_root_password}" <<EOF 2>/dev/null
DELETE FROM mysql.user WHERE User='';
DELETE FROM mysql.user WHERE User='root' AND Host NOT IN ('localhost', '127.0.0.1', '::1');
DROP DATABASE IF EXISTS test;
DELETE FROM mysql.db WHERE Db='test' OR Db='test\\_%';
FLUSH PRIVILEGES;
EOF

    if [ $? -eq 0 ]; then
        print success "MySQL secured successfully"
        log_action "MySQL installation secured"
    else
        print warning "MySQL security setup had some issues, but continuing..."
        log_action "MySQL security setup completed with warnings"
    fi
}

#=============================================================================
# PHP SETUP FUNCTIONS
#=============================================================================

install_php() {
    local php_version=${1:-$(default_php_version_for_os)}
    local force_flag=${2:-}
    print header "Installing and configuring PHP $php_version..."

    # Download switch-php script if not already present or in force mode
    if [ ! -f "/usr/local/bin/switch-php" ] || [ "$force_flag" = "-f" ] || [ "$force_flag" = "--force" ]; then
        print info "Downloading switch-php script..."
        wget https://raw.githubusercontent.com/deforay/utility-scripts/master/php/switch-php -O /usr/local/bin/switch-php
        chmod u+x /usr/local/bin/switch-php
    else
        print info "switch-php script already exists"
    fi

    # Check if the specific PHP version is already active and not in force mode
    if [ "$force_flag" != "-f" ] && [ "$force_flag" != "--force" ]; then
        if command -v php &>/dev/null; then
            current_version=$(php -v | head -n1 | grep -oP 'PHP \K[0-9]+\.[0-9]+')
            if [ "$current_version" == "$php_version" ]; then
                print info "PHP $php_version is already active"
                return 0
            fi
        fi
    fi

    # Run switch-php with or without force flag
    if [ "$force_flag" = "-f" ] || [ "$force_flag" = "--force" ]; then
        print info "Switching to PHP $php_version (force mode)..."
        switch-php $php_version --force
    else
        print info "Switching to PHP $php_version..."
        switch-php $php_version
    fi
}

#=============================================================================
# PHPMYADMIN SETUP FUNCTIONS
#=============================================================================

install_phpmyadmin() {
    local force_flag=${1:-}
    print header "Setting up phpMyAdmin..."

    # Check if force reinstall is requested
    if [ "$force_flag" = "-f" ] || [ "$force_flag" = "--force" ]; then
        if [ -d "/var/www/phpmyadmin" ]; then
            print info "Force mode: Removing existing phpMyAdmin installation..."
            rm -rf /var/www/phpmyadmin
        fi
    fi

    if [ ! -d "/var/www/phpmyadmin" ] || [ -z "$(ls -A /var/www/phpmyadmin 2>/dev/null)" ]; then
        print info "Downloading and setting up phpMyAdmin..."

        mkdir -p /var/www/phpmyadmin

        wget -q --show-progress --progress=dot:giga https://www.phpmyadmin.net/downloads/phpMyAdmin-latest-all-languages.zip

        unzip -q phpMyAdmin-latest-all-languages.zip -d /var/www/phpmyadmin || {
            print error "Extraction failed"
            exit 1
        }

        rm phpMyAdmin-latest-all-languages.zip

        # Move files up one level
        PHPMYADMIN_DIR=$(ls /var/www/phpmyadmin)
        mv /var/www/phpmyadmin/$PHPMYADMIN_DIR/* /var/www/phpmyadmin/
        mv /var/www/phpmyadmin/$PHPMYADMIN_DIR/.[!.]* /var/www/phpmyadmin/ 2>/dev/null
        rmdir /var/www/phpmyadmin/$PHPMYADMIN_DIR
        
        print success "phpMyAdmin installed successfully"
    else
        print info "phpMyAdmin is already installed. Skipping installation. (Use --force to reinstall)"
    fi
}

configure_phpmyadmin_apache() {
    print info "Checking Apache configuration for phpMyAdmin..."

    desired_alias="Alias /phpmyadmin /var/www/phpmyadmin"
    config_file="/etc/apache2/sites-available/000-default.conf"

    if grep -q "$desired_alias" ${config_file}; then
        print info "phpMyAdmin alias already exists in Apache configuration."
    else
        print info "Adding phpMyAdmin alias to Apache configuration..."
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

        service apache2 restart || apache_start_failed
    fi
}

#=============================================================================
# MAIN FUNCTION
#=============================================================================

main() {
    # Error trap
    trap 'error_handling "${BASH_COMMAND}" "$LINENO" "$?"' ERR

    # Parse arguments
    local PHP_VERSION=${1:-$(default_php_version_for_os)}
    local FORCE_FLAG=""

    # Check for force flag in any position
    for arg in "$@"; do
        if [ "$arg" = "-f" ] || [ "$arg" = "--force" ]; then
            FORCE_FLAG="--force"
            print info "Force mode enabled: will reinstall PHP, extensions, Composer, and phpMyAdmin"
            break
        fi
    done

    # If force flag is present and first arg is the flag, use the OS default.
    if [ "$1" = "-f" ] || [ "$1" = "--force" ]; then
        PHP_VERSION="$(default_php_version_for_os)"
    fi

    print header "Starting LAMP Setup Script"
    log_action "LAMP Setup started with PHP version: $PHP_VERSION (Force: ${FORCE_FLAG:-no})"

    # Phase 1: Validation
    check_root_privileges
    is_valid_ubuntu || exit 1
    check_dependencies

    # Phase 2: System Setup
    setup_system
    setup_locale
    setup_ssh

    # Phase 3: Apache Setup
    install_apache
    setup_brotli
    configure_basic_performance
    setup_web_permissions

    # Phase 4: MySQL Setup
    get_mysql_password
    install_mysql
    configure_mysql
    secure_mysql

    # Phase 5: PHP Setup
    install_php $PHP_VERSION $FORCE_FLAG

    # Phase 6: phpMyAdmin Setup
    install_phpmyadmin $FORCE_FLAG
    configure_phpmyadmin_apache

    # Final restart and completion
    service apache2 restart || apache_start_failed

    log_action "LAMP Setup complete."
    print success "LAMP Setup complete successfully!"
    print info "You can access phpMyAdmin at: http://your-server/phpmyadmin"
}

#=============================================================================
# SCRIPT EXECUTION
#=============================================================================

# Only execute main if script is run directly (not sourced)
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi
