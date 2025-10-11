#!/usr/bin/env bash
# db-tools.sh — MySQL/MariaDB admin toolkit
# Features: backups, PITR, encryption, notifications, health checks, GFS rotation
#
# Install:
#   sudo wget -O /usr/local/bin/db-tools https://raw.githubusercontent.com/deforay/utility-scripts/master/db-tools.sh
#   sudo chmod +x /usr/local/bin/db-tools
#
# Usage:
#   db-tools init
#   db-tools backup [full|incremental]
#   db-tools restore <DB|ALL|file.sql.gz> [target-db]
#   db-tools verify
#   db-tools health
#   db-tools sizes
#   db-tools tune
#   db-tools maintain [quick|full]
#   db-tools cleanup [days]

set -euo pipefail

# ========================== Configuration ==========================
CONFIG_FILE="${CONFIG_FILE:-/etc/db-tools.conf}"
BACKUP_DIR="${BACKUP_DIR:-/var/backups/mysql}"
RETENTION_DAYS="${RETENTION_DAYS:-7}"
LOGIN_PATH="${LOGIN_PATH:-dbtools}"
PARALLEL_JOBS="${PARALLEL_JOBS:-2}"
TOP_N_TABLES="${TOP_N_TABLES:-30}"
MARK_DIR="/var/lib/dbtools"
MARK_INIT="$MARK_DIR/init.stamp"
LOCK_FILE="/var/run/db-tools.lock"
if [[ ! -w "$(dirname "$LOCK_FILE")" ]]; then
    LOCK_FILE="/tmp/db-tools.lock"
fi

# Compression settings
COMPRESS_ALGO="${COMPRESS_ALGO:-pigz}"  # pigz, gzip, zstd, xz
COMPRESS_LEVEL="${COMPRESS_LEVEL:-6}"

# Backup types and retention
BACKUP_TYPE="${BACKUP_TYPE:-full}"  # full, incremental
KEEP_DAILY="${KEEP_DAILY:-7}"
KEEP_WEEKLY="${KEEP_WEEKLY:-4}"
KEEP_MONTHLY="${KEEP_MONTHLY:-6}"
CLEAN_KEEP_MIN="${CLEAN_KEEP_MIN:-2}"

# Encryption
ENCRYPT_BACKUPS="${ENCRYPT_BACKUPS:-0}"
ENCRYPTION_KEY_FILE="${ENCRYPTION_KEY_FILE:-}"

# Notifications
NOTIFY_EMAIL="${NOTIFY_EMAIL:-}"
NOTIFY_WEBHOOK="${NOTIFY_WEBHOOK:-}"
NOTIFY_ON="${NOTIFY_ON:-error}"  # always, error, never

# Logging
LOG_LEVEL="${LOG_LEVEL:-INFO}"  # DEBUG, INFO, WARN, ERROR
USE_SYSLOG="${USE_SYSLOG:-0}"

# Other options
DRY_RUN="${DRY_RUN:-0}"
DROP_FIRST="${DROP_FIRST:-0}"
CHECKSUM_ENABLED="${CHECKSUM_ENABLED:-1}"

# Tool paths
MYSQL="${MYSQL:-$(command -v mysql || true)}"
MYSQLDUMP="${MYSQLDUMP:-$(command -v mysqldump || true)}"
MYSQLBINLOG="${MYSQLBINLOG:-$(command -v mysqlbinlog || true)}"

# XtraBackup settings
XTRABACKUP_ENABLED="${XTRABACKUP_ENABLED:-1}"
XTRABACKUP_PARALLEL="${XTRABACKUP_PARALLEL:-$PARALLEL_JOBS}"
XTRABACKUP_COMPRESS="${XTRABACKUP_COMPRESS:-1}"
XTRABACKUP_COMPRESS_THREADS="${XTRABACKUP_COMPRESS_THREADS:-$PARALLEL_JOBS}"
XTRABACKUP_MEMORY="${XTRABACKUP_MEMORY:-1G}"  # Memory for prepare phase
BACKUP_METHOD="${BACKUP_METHOD:-xtrabackup}"  # xtrabackup or mysqldump
XTRABACKUP="${XTRABACKUP:-$(command -v xtrabackup || command -v mariabackup || true)}"

# Global state
declare -g BACKUP_SUMMARY=()
declare -g OPERATION_START=$(date +%s)

# ========================== Utility Functions ==========================

# ---------- TTY helpers (visual cues) ----------
is_tty() { [[ -t 2 ]] && [[ "${NO_TTY:-0}" != "1" ]]; }

SPINNER_PID=""
start_spinner() {
  is_tty || return 0
  local msg="${*:-Working}"
  # minimal spinner on STDERR so normal output stays clean
  (
    local frames='|/-\'
    local i=0
    # hide cursor
    printf '\033[?25l' >&2
    while :; do
      printf '\r%s %s' "$msg" "${frames:i++%4:1}" >&2
      sleep 0.15
    done
  ) &
  SPINNER_PID=$!
  # make sure it stops even on errors
  stack_trap 'stop_spinner' EXIT
}

stop_spinner() {
  [[ -n "$SPINNER_PID" ]] || return 0
  kill "$SPINNER_PID" >/dev/null 2>&1 || true
  wait "$SPINNER_PID" 2>/dev/null || true
  SPINNER_PID=""
  # clear spinner line & show cursor
  printf '\r\033[K\033[?25h' >&2
}

show_progress() {
    local current="$1"
    local total="$2"
    local desc="${3:-Progress}"
    local width=42
    
    [[ "$total" -eq 0 ]] && return
    
    # Cap current at total to prevent overflow
    [[ "$current" -gt "$total" ]] && current="$total"
    
    local pct=$(( current * 100 / total ))
    local filled=$(( width * current / total ))
    local empty=$(( width - filled ))

    # ▓ bar if TTY; plain if not
    if is_tty; then
        printf '\r%s: [%*s%*s] %3d%% (%d/%d)' \
          "$desc" \
          "$filled" '' \
          "$empty" '' \
          "$pct" "$current" "$total" >&2
        # replace spaces in the filled section with █
        printf '\e[%dD' $(( 1 + 1 + 1 + empty + 6 + ${#desc} + 4 )) >&2
        printf '\e[%dC' $(( ${#desc} + 3 )) >&2
        printf '%*s' "$filled" '' | tr ' ' '█' >&2
    else
        printf '%s: %d/%d (%d%%)\n' "$desc" "$current" "$total" "$pct" >&2
    fi

    [[ "$current" -eq "$total" ]] && { is_tty && echo >&2; }
}


log() {
    local level="${1:-INFO}"
    shift
    local msg="$*"
    local ts="$(date +'%F %T')"
    
    case "$level" in
        DEBUG) [[ "$LOG_LEVEL" == "DEBUG" ]] || return 0 ;;
        INFO|WARN|ERROR) ;;
        *) level="INFO" ;;
    esac
    
    local color=""
    case "$level" in
        ERROR) color="\033[0;31m" ;;  # Red
        WARN)  color="\033[0;33m" ;;  # Yellow
        INFO)  color="\033[0;32m" ;;  # Green
        DEBUG) color="\033[0;36m" ;;  # Cyan
    esac
    
    printf "${color}[%s] %s: %s\033[0m\n" "$ts" "$level" "$msg" >&2
    
    [[ "$USE_SYSLOG" == "1" ]] && logger -t db-tools -p "user.${level,,}" "$msg" || true
}

warn() { log WARN "$@"; }
err() { log ERROR "$@"; exit 1; }
debug() { log DEBUG "$@"; }

have() { command -v "$1" >/dev/null 2>&1; }

add_summary() { BACKUP_SUMMARY+=("$*"); }

print_summary() {
    [[ ${#BACKUP_SUMMARY[@]} -eq 0 ]] && return
    local duration=$(( $(date +%s) - OPERATION_START ))
    log INFO "=== Operation Summary (${duration}s) ==="
    printf '%s\n' "${BACKUP_SUMMARY[@]}" >&2
}

generate_encryption_key() {
    local key_file="${1:-$ENCRYPTION_KEY_FILE}"
    
    if [[ -z "$key_file" ]]; then
        err "No encryption key file specified. Set ENCRYPTION_KEY_FILE or provide path as argument."
    fi
    
    if [[ -f "$key_file" ]]; then
        read -r -p "Key file exists at $key_file. Overwrite? [y/N]: " confirm
        [[ "$confirm" =~ ^[Yy]$ ]] || { log INFO "Cancelled"; return 0; }
    fi
    
    local key_dir
    key_dir="$(dirname "$key_file")"
    mkdir -p "$key_dir" || err "Cannot create directory: $key_dir"
    
    log INFO "Generating 256-bit encryption key..."
    
    # Generate a strong random key
    if have openssl; then
        openssl rand -base64 32 > "$key_file"
    elif have /dev/urandom; then
        head -c 32 /dev/urandom | base64 > "$key_file"
    else
        err "Cannot generate random key: neither openssl nor /dev/urandom available"
    fi
    
    # Set secure permissions
    chmod 600 "$key_file"
    
    # Try to set immutable flag if possible (Linux only)
    if have chattr; then
        chattr +i "$key_file" 2>/dev/null && log INFO "Set immutable flag on key file" || true
    fi
    
    log INFO "✅ Encryption key created: $key_file"
    log INFO "⚠️  IMPORTANT: Back up this key file securely!"
    log INFO "⚠️  Without this key, encrypted backups cannot be restored."
    echo
    log INFO "To enable encryption, set in your config:"
    echo "  ENCRYPT_BACKUPS=1"
    echo "  ENCRYPTION_KEY_FILE=\"$key_file\""
}

trap print_summary EXIT

# ========================== Lock Management ==========================

acquire_lock() {
  local timeout="${1:-300}"

  if have flock; then
    # Use FD 9 for the lock; keep it open for the process lifetime
    exec 9>"$LOCK_FILE"
    if ! flock -n 9; then
      err "Another db-tools instance holds the lock ($LOCK_FILE)."
    fi
    stack_trap 'release_lock' EXIT
    debug "Lock acquired via flock (PID: $$)"
    return
  fi

  # Fallback to manual lock file with timeout
  local elapsed=0
  while [[ -f "$LOCK_FILE" ]]; do
    if (( elapsed >= timeout )); then
      local pid=$(cat "$LOCK_FILE" 2>/dev/null || echo "unknown")
      err "Lock file exists (PID: $pid). Another instance running or stale lock?"
    fi
    debug "Waiting for lock... (${elapsed}s)"
    sleep 5
    ((elapsed+=5))
  done

  echo $$ > "$LOCK_FILE"
  stack_trap 'release_lock' EXIT
  debug "Lock acquired (PID: $$)"
}

release_lock() {
  # Close FD if flock path used
  { exec 9>&-; } 2>/dev/null || true
  rm -f "$LOCK_FILE"
  debug "Lock released"
}

# ========================== Configuration Loading ==========================

load_config() {
    if [[ -f "$CONFIG_FILE" ]]; then
        # shellcheck disable=SC1090
        source "$CONFIG_FILE"
        debug "Configuration loaded from $CONFIG_FILE"
    fi
}

# ========================== Tool Checking ==========================

need_tooling() {
    [[ -n "$MYSQL" ]]     || err "mysql client not found"
    [[ -n "$MYSQLDUMP" ]] || warn "mysqldump not found (logical backups will be unavailable)"
    [[ -n "$MYSQLBINLOG" ]] || warn "mysqlbinlog not found (PITR will be limited)"
    
    if [[ "$BACKUP_METHOD" == "xtrabackup" ]]; then
        if [[ -z "$XTRABACKUP" ]]; then
            warn "xtrabackup/mariabackup not found, falling back to mysqldump"
            BACKUP_METHOD="mysqldump"
        else
            debug "Using XtraBackup at: $XTRABACKUP"
        fi
    fi
}

ensure_compression_tools() {
    case "$COMPRESS_ALGO" in
        pigz)
            if ! have pigz; then
                warn "pigz not found, attempting install..."
                install_package pigz || warn "Failed to install pigz, falling back to gzip"
            fi
            ;;
        zstd)
            if ! have zstd; then
                warn "zstd not found, attempting install..."
                install_package zstd || warn "Failed to install zstd, falling back to gzip"
            fi
            ;;
        xz)
            if ! have xz; then
                warn "xz-utils not found"
                install_package xz-utils || install_package xz || warn "Failed to install xz; falling back to gzip"
            fi
            ;;
    esac
}

# Stack new commands onto an existing trap on a signal (default EXIT)
stack_trap() {
  local new_cmd="$1" sig="${2:-EXIT}"
  local old_cmd
  old_cmd="$(trap -p "$sig" | sed -E "s/.*'(.+)'/\1/")"
  if [[ -n "$old_cmd" && "$old_cmd" != "trap -- '' $sig" ]]; then
    trap "$old_cmd; $new_cmd" "$sig"
  else
    trap "$new_cmd" "$sig"
  fi
}

check_key_perms() {
  [[ "$ENCRYPT_BACKUPS" == "1" && -n "$ENCRYPTION_KEY_FILE" ]] || return 0
  
  [[ -f "$ENCRYPTION_KEY_FILE" ]] || err "Encryption key file not found: $ENCRYPTION_KEY_FILE"
  
  # Check permissions - must be 600 or 400
  if perms=$(stat -c '%a' "$ENCRYPTION_KEY_FILE" 2>/dev/null); then
    if (( perms > 600 )); then
      err "SECURITY: Encryption key permissions are $perms (must be 600 or stricter). Fix with: chmod 600 $ENCRYPTION_KEY_FILE"
    fi
  elif perms=$(stat -f '%Lp' "$ENCRYPTION_KEY_FILE" 2>/dev/null); then
    # BSD stat fallback
    if (( perms > 600 )); then
      err "SECURITY: Encryption key permissions are $perms (must be 600 or stricter). Fix with: chmod 600 $ENCRYPTION_KEY_FILE"
    fi
  fi
  
  # Check ownership (should be current user or root)
  if owner=$(stat -c '%U' "$ENCRYPTION_KEY_FILE" 2>/dev/null); then
    if [[ "$owner" != "$USER" && "$owner" != "root" && "$EUID" -ne 0 ]]; then
      warn "Encryption key owned by $owner (expected $USER or root)"
    fi
  fi
  
  debug "Encryption key permissions validated"
}

install_xtrabackup() {
    [[ "$EUID" -ne 0 ]] && { warn "Cannot auto-install without root"; return 1; }
    
    log INFO "Installing XtraBackup/MariaBackup..."
    
    if have apt-get; then
        export DEBIAN_FRONTEND=noninteractive
        
        # Detect if MariaDB or MySQL
        if "$MYSQL" --version 2>/dev/null | grep -qi mariadb; then
            log INFO "Detected MariaDB, installing mariadb-backup..."
            apt-get update -y >/dev/null 2>&1 || true
            if apt-get install -y mariadb-backup 2>/dev/null; then
                log INFO "✅ mariadb-backup installed"
            else
                warn "Failed to install mariadb-backup"
                return 1
            fi
        else
            log INFO "Detected MySQL, installing percona-xtrabackup..."
            # Try Percona repository
            if ! have percona-release; then
                wget -q https://repo.percona.com/apt/percona-release_latest.generic_all.deb -O /tmp/percona-release.deb 2>/dev/null || {
                    warn "Cannot download percona-release"
                    return 1
                }
                dpkg -i /tmp/percona-release.deb >/dev/null 2>&1 || true
                rm -f /tmp/percona-release.deb
            fi
            
            percona-release enable-only tools release >/dev/null 2>&1 || true
            apt-get update -y >/dev/null 2>&1 || true
            
            # Try different versions
            if apt-get install -y percona-xtrabackup-80 2>/dev/null; then
                log INFO "✅ percona-xtrabackup-80 installed"
            elif apt-get install -y percona-xtrabackup-24 2>/dev/null; then
                log INFO "✅ percona-xtrabackup-24 installed"
            else
                warn "Failed to install percona-xtrabackup"
                return 1
            fi
        fi
    elif have yum; then
        if "$MYSQL" --version 2>/dev/null | grep -qi mariadb; then
            yum -y install MariaDB-backup >/dev/null 2>&1
        else
            yum install -y https://repo.percona.com/yum/percona-release-latest.noarch.rpm >/dev/null 2>&1 || true
            percona-release enable-only tools release >/dev/null 2>&1 || true
            yum -y install percona-xtrabackup-80 >/dev/null 2>&1 || \
            yum -y install percona-xtrabackup-24 >/dev/null 2>&1
        fi
    elif have dnf; then
        if "$MYSQL" --version 2>/dev/null | grep -qi mariadb; then
            dnf -y install MariaDB-backup >/dev/null 2>&1
        else
            dnf install -y https://repo.percona.com/yum/percona-release-latest.noarch.rpm >/dev/null 2>&1 || true
            percona-release enable-only tools release >/dev/null 2>&1 || true
            dnf -y install percona-xtrabackup-80 >/dev/null 2>&1 || \
            dnf -y install percona-xtrabackup-24 >/dev/null 2>&1
        fi
    else
        warn "Unsupported package manager"
        return 1
    fi
    
    # Update path
    XTRABACKUP="$(command -v xtrabackup || command -v mariabackup || true)"
    if [[ -n "$XTRABACKUP" ]]; then
        log INFO "✅ XtraBackup installed at: $XTRABACKUP"
        # Update backup method
        BACKUP_METHOD="xtrabackup"
        return 0
    else
        warn "XtraBackup installation completed but binary not found"
        return 1
    fi
}

install_package() {
    local pkg="$1"
    [[ "$EUID" -ne 0 ]] && { warn "Cannot auto-install without root"; return 1; }
    
    if have apt-get; then
        export DEBIAN_FRONTEND=noninteractive
        apt-get update -y >/dev/null 2>&1 || true
        apt-get install -y "$pkg" >/dev/null 2>&1
    elif have yum; then
        yum -y install "$pkg" >/dev/null 2>&1
    elif have dnf; then
        dnf -y install "$pkg" >/dev/null 2>&1
    else
        return 1
    fi
}

# ========================== Compression Functions ==========================

compressor() {
    local cores=$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo 1)
    
    case "$COMPRESS_ALGO" in
        pigz)
            if have pigz; then
                echo "pigz -p $cores -${COMPRESS_LEVEL} -c"
                return
            fi
            ;;
        zstd)
            if have zstd; then
                echo "zstd -${COMPRESS_LEVEL} -T0 -c"
                return
            fi
            ;;
        xz)
            if have xz; then
                echo "xz -${COMPRESS_LEVEL} -T0 -c"
                return
            fi
            ;;
    esac
    
    echo "gzip -${COMPRESS_LEVEL} -c"
}

decompressor() {
    local file="$1"
    
    if [[ "$file" =~ \.zst$ ]]; then
        echo "zstd -d -c"
    elif [[ "$file" =~ \.xz$ ]]; then
        echo "xz -d -c"
    else
        echo "gzip -d -c"
    fi
}

get_extension() {
    case "$COMPRESS_ALGO" in
        zstd) echo "sql.zst" ;;
        xz)   echo "sql.xz" ;;
        *)    echo "sql.gz" ;;
    esac
}

# ========================== Encryption Functions ==========================

encrypt_if_enabled() {
    if [[ "$ENCRYPT_BACKUPS" == "1" ]] && [[ -n "$ENCRYPTION_KEY_FILE" ]] && [[ -f "$ENCRYPTION_KEY_FILE" ]]; then
        openssl enc -aes-256-cbc -salt -pbkdf2 -pass file:"$ENCRYPTION_KEY_FILE"
    else
        cat
    fi
}

decrypt_if_encrypted() {
    local file="$1"
    if [[ "$file" =~ \.enc$ ]] && [[ -n "$ENCRYPTION_KEY_FILE" ]] && [[ -f "$ENCRYPTION_KEY_FILE" ]]; then
        openssl enc -d -aes-256-cbc -pbkdf2 -pass file:"$ENCRYPTION_KEY_FILE"
    else
        cat
    fi
}

# ========================== Checksum Functions ==========================

create_checksum() {
    local file="$1"
    [[ "$CHECKSUM_ENABLED" != "1" ]] && return 0
    
    if have sha256sum; then
        sha256sum "$file" > "${file}.sha256"
        debug "Checksum created: ${file}.sha256"
    fi
}

verify_checksum() {
    local file="$1"
    local checksum="${file}.sha256"
    
    [[ "$CHECKSUM_ENABLED" != "1" ]] && return 0
    [[ ! -f "$checksum" ]] && return 0
    
    if sha256sum -c "$checksum" >/dev/null 2>&1; then
        debug "Checksum valid: $(basename "$file")"
        return 0
    else
        warn "Checksum mismatch: $(basename "$file")"
        return 1
    fi
}

# ========================== Notification Functions ==========================

notify() {
    local subject="$1"
    local message="$2"
    local level="${3:-info}"
    
    [[ "$NOTIFY_ON" == "never" ]] && return 0
    [[ "$NOTIFY_ON" == "error" && "$level" != "error" ]] && return 0
    
    if [[ -n "$NOTIFY_EMAIL" ]] && have mail; then
        echo "$message" | mail -s "db-tools: $subject" "$NOTIFY_EMAIL" || true
    fi
    
    if [[ -n "$NOTIFY_WEBHOOK" ]] && have curl; then
        curl -s -X POST "$NOTIFY_WEBHOOK" \
            -H "Content-Type: application/json" \
            -d "{\"subject\":\"$subject\",\"message\":\"$message\",\"level\":\"$level\",\"host\":\"$(hostname)\"}" \
            >/dev/null 2>&1 || true
    fi
}

# ========================== MySQL Functions ==========================

require_login() {
    if ! "$MYSQL" --login-path="$LOGIN_PATH" -e "SELECT 1;" >/dev/null 2>&1; then
        err "MySQL --login-path='$LOGIN_PATH' failed. Run '$0 init' first."
    fi
}

dump_ts_from_name() {
    local base="$1"
    if [[ "$base" =~ ([0-9]{4}-[0-9]{2}-[0-9]{2}-[0-9]{2}-[0-9]{2}-[0-9]{2}) ]]; then
        echo "${BASH_REMATCH[1]}"
    fi
}

# ========================== Disk Space Functions ==========================

check_disk_space() {
    local dir="$1"
    local required_mb="${2:-1000}"
    
    local available=$(df -BM "$dir" | awk 'NR==2 {print $4}' | sed 's/M//')
    local recommended=$((required_mb * 2))
    
    if (( available < recommended )); then
        warn "Low disk space. Available: ${available}MB, Recommended: ${recommended}MB"
        if (( available < required_mb )); then
            err "Insufficient disk space. Operation aborted."
        fi
    fi
    
    debug "Disk space check: ${available}MB available"
}

estimate_backup_size() {
    local total_mb=$("$MYSQL" --login-path="$LOGIN_PATH" -N -e "
        SELECT COALESCE(ROUND(SUM(data_length+index_length)/1024/1024,0), 0)
        FROM information_schema.TABLES
        WHERE table_schema NOT IN ('mysql','information_schema','performance_schema','sys');"
    )
    echo "${total_mb:-0}"
}

# ========================== Progress Functions ==========================

# ========================== Initialization ==========================

init() {
    need_tooling
    log INFO "Initializing db-tools with login-path '$LOGIN_PATH'..."
    
    have mysql_config_editor || err "mysql_config_editor not found"
    
    read -r -p "MySQL host [localhost]: " host; host="${host:-localhost}"
    read -r -p "MySQL port [3306]: " port; port="${port:-3306}"
    read -r -p "MySQL admin user [root]: " user; user="${user:-root}"
    read -r -s -p "MySQL password for '$user': " pass; echo
    
    mysql_config_editor set \
        --login-path="$LOGIN_PATH" \
        --host="$host" \
        --user="$user" \
        --port="$port" \
        --password <<<"$pass"
    
    "$MYSQL" --login-path="$LOGIN_PATH" -e "SELECT VERSION();" >/dev/null || err "Login test failed"
    
    log INFO "Installing additional tools..."
    if have apt-get; then
        export DEBIAN_FRONTEND=noninteractive
        apt-get update -y >/dev/null 2>&1 || true
        apt-get install -y percona-toolkit mysqltuner mailutils qpress >/dev/null 2>&1 || warn "Some tools failed to install"
    elif have yum; then
        yum -y install percona-toolkit mysqltuner mailx qpress >/dev/null 2>&1 || warn "Some tools failed to install"
    fi
    
    # *** ADD THIS SECTION ***
    log INFO "Installing XtraBackup..."
    if install_xtrabackup; then
        log INFO "✅ XtraBackup installation successful"
    else
        warn "XtraBackup installation failed - will use mysqldump instead"
        BACKUP_METHOD="mysqldump"
    fi
    # *** END NEW SECTION ***
    
    ensure_compression_tools
    check_key_perms
    
    mkdir -p "$BACKUP_DIR" "$MARK_DIR"
    date -Is > "$MARK_INIT"
    
    log INFO "✅ Initialization complete"
    add_summary "Initialized login-path: $LOGIN_PATH"
}

# ========================== Backup Functions ==========================

backup() {
    local backup_type="${1:-full}"
    
    need_tooling
    require_login
    ensure_compression_tools
    check_key_perms
    acquire_lock
    
    local estimated_size=$(estimate_backup_size)
    check_disk_space "$BACKUP_DIR" "$estimated_size"
    
    case "$backup_type" in
        full)
            if [[ "$BACKUP_METHOD" == "xtrabackup" && -n "$XTRABACKUP" ]]; then
                backup_xtra_full
            else
                backup_full
            fi
            ;;
        incremental)
            if [[ "$BACKUP_METHOD" == "xtrabackup" && -n "$XTRABACKUP" ]]; then
                backup_xtra_incremental
            else
                backup_incremental
            fi
            ;;
        logical)
            backup_full
            ;;
        *)
            err "Unknown backup type: $backup_type (use: full, incremental, logical)"
            ;;
    esac
}

backup_xtra_full() {
    local ts="$(date +'%Y-%m-%d-%H-%M-%S')"
    local backup_dir="$BACKUP_DIR/xtra-full-$ts"
    
    log INFO "Starting XtraBackup full backup..."
    
    # Cleanup old backups first
    cleanup_old_backups
    
    mkdir -p "$backup_dir"
    
    # Build xtrabackup command
    local xtra_cmd=("$XTRABACKUP" --backup --target-dir="$backup_dir")
    xtra_cmd+=(--parallel="$XTRABACKUP_PARALLEL")
    
    # Add compression if enabled
    if [[ "$XTRABACKUP_COMPRESS" == "1" ]]; then
        case "$COMPRESS_ALGO" in
            zstd)
                if have zstd; then
                    xtra_cmd+=(--compress=zstd)
                    xtra_cmd+=(--compress-threads="$XTRABACKUP_COMPRESS_THREADS")
                fi
                ;;
            *)
                # XtraBackup has built-in qpress compression
                if have qpress; then
                    xtra_cmd+=(--compress)
                    xtra_cmd+=(--compress-threads="$XTRABACKUP_COMPRESS_THREADS")
                fi
                ;;
        esac
    fi
    
    # Get MySQL credentials from login-path
    local mysql_user mysql_pass mysql_host mysql_port
    mysql_host=$(mysql_config_editor print --login-path="$LOGIN_PATH" 2>/dev/null | grep host | cut -d= -f2 | tr -d ' ')
    mysql_port=$(mysql_config_editor print --login-path="$LOGIN_PATH" 2>/dev/null | grep port | cut -d= -f2 | tr -d ' ')
    mysql_user=$(mysql_config_editor print --login-path="$LOGIN_PATH" 2>/dev/null | grep user | cut -d= -f2 | tr -d ' ')
    
    [[ -n "$mysql_host" ]] && xtra_cmd+=(--host="$mysql_host")
    [[ -n "$mysql_port" ]] && xtra_cmd+=(--port="$mysql_port")
    [[ -n "$mysql_user" ]] && xtra_cmd+=(--user="$mysql_user")
    
    # Run backup
    log INFO "Executing: ${xtra_cmd[*]}"
    
    if "${xtra_cmd[@]}" 2>"$backup_dir/xtrabackup.log"; then
        # Save metadata
        {
            echo "timestamp=$ts"
            echo "backup_type=xtrabackup-full"
            echo "backup_method=xtrabackup"
            echo "xtrabackup_version=$($XTRABACKUP --version 2>&1 | head -1)"
            echo "server_version=$("$MYSQL" --login-path="$LOGIN_PATH" -N -e 'SELECT VERSION();')"
            
            # Extract LSN from xtrabackup_checkpoints
            if [[ -f "$backup_dir/xtrabackup_checkpoints" ]]; then
                grep "^to_lsn" "$backup_dir/xtrabackup_checkpoints" >> "$BACKUP_DIR/backup-$ts.meta"
            fi
            
            # Get binlog position
            if [[ -f "$backup_dir/xtrabackup_binlog_info" ]]; then
                local binlog_info
                binlog_info=$(cat "$backup_dir/xtrabackup_binlog_info")
                echo "binlog_file=$(echo "$binlog_info" | awk '{print $1}')" 
                echo "binlog_pos=$(echo "$binlog_info" | awk '{print $2}')"
            fi
        } > "$BACKUP_DIR/backup-$ts.meta"
        
        # Encrypt if enabled
        if [[ "$ENCRYPT_BACKUPS" == "1" ]]; then
            log INFO "Encrypting backup..."
            tar -cf - -C "$BACKUP_DIR" "xtra-full-$ts" | \
                encrypt_if_enabled > "$BACKUP_DIR/xtra-full-$ts.tar.enc"
            rm -rf "$backup_dir"
            backup_dir="$BACKUP_DIR/xtra-full-$ts.tar.enc"
        else
            # Tar it for easier management
            tar -czf "$BACKUP_DIR/xtra-full-$ts.tar.gz" -C "$BACKUP_DIR" "xtra-full-$ts"
            rm -rf "$backup_dir"
            backup_dir="$BACKUP_DIR/xtra-full-$ts.tar.gz"
        fi
        
        # Create checksum
        create_checksum "$backup_dir"
        
        log INFO "✅ XtraBackup full backup complete: $(basename "$backup_dir")"
        add_summary "XtraBackup full backup: $(basename "$backup_dir")"
        notify "Backup completed successfully" "XtraBackup full backup completed" "info"
        return 0
    else
        err "XtraBackup full backup failed. Check $backup_dir/xtrabackup.log"
    fi
}

backup_xtra_incremental() {
    local ts="$(date +'%Y-%m-%d-%H-%M-%S')"
    
    # Find base backup
    local base_backup
    base_backup=$(find "$BACKUP_DIR" -maxdepth 1 \( -name "xtra-full-*.tar.gz" -o -name "xtra-full-*.tar.enc" \) 2>/dev/null | sort -r | head -1)
    
    if [[ -z "$base_backup" ]]; then
        warn "No full XtraBackup found, creating full backup instead"
        backup_xtra_full
        return
    fi
    
    log INFO "Starting XtraBackup incremental backup..."
    
    # Extract base backup to temp location
    local temp_base="$BACKUP_DIR/.xtra_base_$$"
    mkdir -p "$temp_base"
    stack_trap "rm -rf '$temp_base'" EXIT
    
    if [[ "$base_backup" =~ \.enc$ ]]; then
        decrypt_if_encrypted "$base_backup" < "$base_backup" | tar -xzf - -C "$temp_base"
    else
        tar -xzf "$base_backup" -C "$temp_base"
    fi
    
    local base_dir=$(find "$temp_base" -maxdepth 1 -type d -name "xtra-full-*" | head -1)
    [[ -z "$base_dir" ]] && err "Cannot find extracted base backup"
    
    local backup_dir="$BACKUP_DIR/xtra-incr-$ts"
    mkdir -p "$backup_dir"
    
    # Build incremental command
    local xtra_cmd=("$XTRABACKUP" --backup)
    xtra_cmd+=(--target-dir="$backup_dir")
    xtra_cmd+=(--incremental-basedir="$base_dir")
    xtra_cmd+=(--parallel="$XTRABACKUP_PARALLEL")
    
    # Add compression
    if [[ "$XTRABACKUP_COMPRESS" == "1" ]] && have qpress; then
        xtra_cmd+=(--compress)
        xtra_cmd+=(--compress-threads="$XTRABACKUP_COMPRESS_THREADS")
    fi
    
    # Get credentials
    local mysql_user mysql_host mysql_port
    mysql_host=$(mysql_config_editor print --login-path="$LOGIN_PATH" 2>/dev/null | grep host | cut -d= -f2 | tr -d ' ')
    mysql_port=$(mysql_config_editor print --login-path="$LOGIN_PATH" 2>/dev/null | grep port | cut -d= -f2 | tr -d ' ')
    mysql_user=$(mysql_config_editor print --login-path="$LOGIN_PATH" 2>/dev/null | grep user | cut -d= -f2 | tr -d ' ')
    
    [[ -n "$mysql_host" ]] && xtra_cmd+=(--host="$mysql_host")
    [[ -n "$mysql_port" ]] && xtra_cmd+=(--port="$mysql_port")
    [[ -n "$mysql_user" ]] && xtra_cmd+=(--user="$mysql_user")
    
    if "${xtra_cmd[@]}" 2>"$backup_dir/xtrabackup.log"; then
        # Save metadata
        {
            echo "timestamp=$ts"
            echo "backup_type=xtrabackup-incremental"
            echo "backup_method=xtrabackup"
            echo "base_backup=$(basename "$base_backup")"
            
            if [[ -f "$backup_dir/xtrabackup_checkpoints" ]]; then
                grep "^to_lsn" "$backup_dir/xtrabackup_checkpoints"
            fi
        } > "$BACKUP_DIR/backup-$ts.meta"
        
        # Package incremental
        if [[ "$ENCRYPT_BACKUPS" == "1" ]]; then
            tar -cf - -C "$BACKUP_DIR" "xtra-incr-$ts" | \
                encrypt_if_enabled > "$BACKUP_DIR/xtra-incr-$ts.tar.enc"
            rm -rf "$backup_dir"
            backup_dir="$BACKUP_DIR/xtra-incr-$ts.tar.enc"
        else
            tar -czf "$BACKUP_DIR/xtra-incr-$ts.tar.gz" -C "$BACKUP_DIR" "xtra-incr-$ts"
            rm -rf "$backup_dir"
            backup_dir="$BACKUP_DIR/xtra-incr-$ts.tar.gz"
        fi
        
        create_checksum "$backup_dir"
        
        log INFO "✅ XtraBackup incremental backup complete: $(basename "$backup_dir")"
        add_summary "XtraBackup incremental backup: $(basename "$backup_dir")"
        notify "Incremental backup completed" "XtraBackup incremental backup completed" "info"
    else
        err "XtraBackup incremental backup failed"
    fi
}

restore_xtra() {
    local backup_file="$1"
    local target_db="${2:-}"
    local until_time="${3:-}"
    
    log INFO "Restoring from XtraBackup: $(basename "$backup_file")"
    
    # Verify checksum
    verify_checksum "$backup_file" || warn "Checksum verification failed"
    
    # Extract backup
    local temp_restore="$BACKUP_DIR/.xtra_restore_$$"
    mkdir -p "$temp_restore"
    stack_trap "rm -rf '$temp_restore'" EXIT
    
    log INFO "Extracting backup..."
    if [[ "$backup_file" =~ \.enc$ ]]; then
        decrypt_if_encrypted "$backup_file" < "$backup_file" | tar -xzf - -C "$temp_restore"
    else
        tar -xzf "$backup_file" -C "$temp_restore"
    fi
    
    local backup_dir=$(find "$temp_restore" -maxdepth 1 -type d -name "xtra-*" | head -1)
    [[ -z "$backup_dir" ]] && err "Cannot find extracted backup"
    
    # Decompress if needed
    if [[ -f "$backup_dir/xtrabackup_checkpoints" ]]; then
        if grep -q "compressed = 1" "$backup_dir/xtrabackup_checkpoints" 2>/dev/null; then
            log INFO "Decompressing backup..."
            "$XTRABACKUP" --decompress --target-dir="$backup_dir" \
                --parallel="$XTRABACKUP_PARALLEL" || err "Decompression failed"
            
            # Remove .qp files
            find "$backup_dir" -name "*.qp" -delete
        fi
    fi
    
    # Apply incrementals if any
    local base_ts
    base_ts=$(basename "$backup_file" | grep -oP '\d{4}-\d{2}-\d{2}-\d{2}-\d{2}-\d{2}')
    
    local incrementals=()
    while IFS= read -r incr; do
        incrementals+=("$incr")
    done < <(find "$BACKUP_DIR" -name "xtra-incr-*.tar.*" 2>/dev/null | sort)
    
    if [[ ${#incrementals[@]} -gt 0 ]]; then
        log INFO "Found ${#incrementals[@]} incremental backup(s) to apply"
        
        # First prepare base with --apply-log-only
        log INFO "Preparing base backup..."
        "$XTRABACKUP" --prepare --apply-log-only \
            --target-dir="$backup_dir" \
            --use-memory="$XTRABACKUP_MEMORY" || err "Base prepare failed"
        
        # Apply each incremental
        for incr in "${incrementals[@]}"; do
            log INFO "Applying incremental: $(basename "$incr")"
            
            local temp_incr="$temp_restore/incr_$$"
            mkdir -p "$temp_incr"
            
            if [[ "$incr" =~ \.enc$ ]]; then
                decrypt_if_encrypted "$incr" < "$incr" | tar -xzf - -C "$temp_incr"
            else
                tar -xzf "$incr" -C "$temp_incr"
            fi
            
            local incr_dir=$(find "$temp_incr" -maxdepth 1 -type d -name "xtra-incr-*" | head -1)
            
            # Decompress incremental if needed
            if [[ -f "$incr_dir/xtrabackup_checkpoints" ]]; then
                if grep -q "compressed = 1" "$incr_dir/xtrabackup_checkpoints" 2>/dev/null; then
                    "$XTRABACKUP" --decompress --target-dir="$incr_dir" \
                        --parallel="$XTRABACKUP_PARALLEL"
                    find "$incr_dir" -name "*.qp" -delete
                fi
            fi
            
            "$XTRABACKUP" --prepare --apply-log-only \
                --target-dir="$backup_dir" \
                --incremental-dir="$incr_dir" \
                --use-memory="$XTRABACKUP_MEMORY" || err "Incremental apply failed"
            
            rm -rf "$temp_incr"
        done
    fi
    
    # Final prepare
    log INFO "Final prepare..."
    "$XTRABACKUP" --prepare \
        --target-dir="$backup_dir" \
        --use-memory="$XTRABACKUP_MEMORY" || err "Final prepare failed"
    
    # Stop MySQL
    log INFO "Stopping MySQL..."
    if systemctl is-active --quiet mysql; then
        systemctl stop mysql || systemctl stop mysqld || err "Cannot stop MySQL"
    elif systemctl is-active --quiet mariadb; then
        systemctl stop mariadb || err "Cannot stop MariaDB"
    else
        err "MySQL/MariaDB service not found"
    fi
    
    # Backup current datadir
    local datadir="/var/lib/mysql"
    local backup_old="$datadir.backup.$(date +%s)"
    log INFO "Backing up current datadir to $backup_old"
    mv "$datadir" "$backup_old" || err "Cannot backup current datadir"
    mkdir -p "$datadir"
    
    # Copy back
    log INFO "Copying backup to datadir..."
    "$XTRABACKUP" --copy-back --target-dir="$backup_dir" || {
        log ERROR "Copy-back failed, restoring original datadir"
        rm -rf "$datadir"
        mv "$backup_old" "$datadir"
        err "Restore failed"
    }
    
    # Fix permissions
    chown -R mysql:mysql "$datadir" || chown -R mysql:mysql "$datadir"
    
    # Start MySQL
    log INFO "Starting MySQL..."
    if systemctl start mysql || systemctl start mysqld || systemctl start mariadb; then
        log INFO "✅ XtraBackup restore complete"
        
        # PITR if requested
        if [[ -n "$until_time" ]]; then
            # Extract binlog info from backup
            if [[ -f "$backup_dir/xtrabackup_binlog_info" ]]; then
                local binlog_file binlog_pos
                binlog_file=$(awk '{print $1}' "$backup_dir/xtrabackup_binlog_info")
                binlog_pos=$(awk '{print $2}' "$backup_dir/xtrabackup_binlog_info")
                
                log INFO "Applying binlogs from $binlog_file:$binlog_pos to $until_time"
                
                # Apply binlogs
                run_pitr_from_position "$binlog_file" "$binlog_pos" "$until_time"
            fi
        fi
        
        add_summary "XtraBackup restore: Success"
    else
        err "Cannot start MySQL after restore"
    fi
}

run_pitr_from_position() {
    local start_binlog="$1"
    local start_pos="$2"
    local until_time="$3"
    
    [[ -z "$MYSQLBINLOG" ]] && { warn "mysqlbinlog not available"; return 1; }
    
    local binlog_dir="/var/lib/mysql"
    local binlogs=()
    
    # Find all binlogs starting from start_binlog
    local found=0
    while IFS= read -r binlog; do
        local binlog_name=$(basename "$binlog")
        if [[ "$binlog_name" == "$start_binlog" ]]; then
            found=1
        fi
        [[ $found -eq 1 ]] && binlogs+=("$binlog")
    done < <(find "$binlog_dir" -name "*.0*" -type f | sort)
    
    [[ ${#binlogs[@]} -eq 0 ]] && { warn "No binlogs found"; return 1; }
    
    log INFO "Applying ${#binlogs[@]} binlog file(s) for PITR"
    
    for i in "${!binlogs[@]}"; do
        local binlog="${binlogs[$i]}"
        local cmd=("$MYSQLBINLOG")
        
        # Start position only for first file
        [[ $i -eq 0 ]] && cmd+=(--start-position="$start_pos")
        
        # Stop datetime for all files
        cmd+=(--stop-datetime="$until_time")
        cmd+=("$binlog")
        
        log INFO "Processing: $(basename "$binlog")"
        "${cmd[@]}" | "$MYSQL" --login-path="$LOGIN_PATH" || warn "Error applying $binlog"
    done
    
    log INFO "✅ PITR complete"
}

backup_full() {
    local ts="$(date +'%Y-%m-%d-%H-%M-%S')"
    local comp="$(compressor)"
    local ext="$(get_extension)"
    [[ "$ENCRYPT_BACKUPS" == "1" ]] && ext="${ext}.enc"
    
    log INFO "Starting full backup..."
    
    # Cleanup old backups first
    cleanup_old_backups
    
    # Get databases to backup
    mapfile -t DBS < <(
        "$MYSQL" --login-path="$LOGIN_PATH" -N -e "SHOW DATABASES;" \
            | grep -Ev '^(information_schema|performance_schema|sys|mysql)$' \
            | sort
    )
    
    [[ ${#DBS[@]} -gt 0 ]] || { log INFO "No databases to back up"; return 0; }
    
    log INFO "Backing up ${#DBS[@]} database(s): ${DBS[*]}"
    
    # Capture PITR information
    local binlog_file="" binlog_pos="" gtid=""
    if "$MYSQL" --login-path="$LOGIN_PATH" -e "SHOW MASTER STATUS\G" >/tmp/masterstatus.$$ 2>/dev/null; then
        binlog_file="$(awk -F': ' '/File:/{print $2}' /tmp/masterstatus.$$ || true)"
        binlog_pos="$(awk -F': ' '/Position:/{print $2}' /tmp/masterstatus.$$ || true)"
        gtid="$(awk -F': ' '/Executed_Gtid_Set:/{print $2}' /tmp/masterstatus.$$ || true)"
        rm -f /tmp/masterstatus.$$
    fi
    
    # Save metadata
    {
        echo "timestamp=$ts"
        echo "backup_type=full"
        echo "binlog_file=$binlog_file"
        echo "binlog_pos=$binlog_pos"
        echo "gtid_set=$gtid"
        echo "server_version=$("$MYSQL" --login-path="$LOGIN_PATH" -N -e 'SELECT VERSION();')"
        echo "compression=$COMPRESS_ALGO"
        echo "encrypted=$ENCRYPT_BACKUPS"
    } > "$BACKUP_DIR/backup-$ts.meta"
    
    # Dump options
    read -r -d '' DUMPOPTS <<'OPTS' || true
--single-transaction
--quick
--routines
--triggers
--events
--hex-blob
--default-character-set=utf8mb4
--set-gtid-purged=OFF
--no-tablespaces
--skip-lock-tables
--master-data=0
--column-statistics=0
OPTS
    
    # Dump function with validation
    dump_one() {
        local db="$1"
        local out="$BACKUP_DIR/${db}-${ts}.${ext}"
        local tmp="${out}.partial"
        
        if $MYSQLDUMP --login-path="$LOGIN_PATH" $DUMPOPTS --databases "$db" 2>"${out}.err" \
            | $comp | encrypt_if_enabled > "$tmp"; then
            mv "$tmp" "$out"
            rm -f "${out}.err"
            create_checksum "$out"
            
            # Quick validation - check if backup is readable and contains MySQL dump header
            local decomp_cmd="$(decompressor "$out")"
            if [[ "$ENCRYPT_BACKUPS" == "1" ]]; then
                if ! decrypt_if_encrypted "$out" < "$out" | $decomp_cmd | head -n 50 | grep -q "^-- MySQL dump" 2>/dev/null; then
                    warn "Backup validation failed: $db"
                    return 1
                fi
            else
                if ! $decomp_cmd < "$out" | head -n 50 | grep -q "^-- MySQL dump" 2>/dev/null; then
                    warn "Backup validation failed: $db"
                    return 1
                fi
            fi
            
            log INFO "✅ $db → $(basename "$out")"
            return 0
        else
            rm -f "$tmp"
            warn "❌ $db failed (see ${out}.err)"
            return 1
        fi
    }
    
    # Execute dumps
    local errors=0
    local completed=0
    local status_dir="$BACKUP_DIR/.job_status.$$"
    mkdir -p "$status_dir"
    stack_trap "rm -rf '$status_dir'" EXIT

    if (( PARALLEL_JOBS > 1 )); then
        log INFO "Using $PARALLEL_JOBS parallel jobs"
        start_spinner "Running backups"

        # --- portable semaphore via FIFO (no wait -n needed) ---
        local fifo
        fifo="$(mktemp -u)"
        mkfifo "$fifo"
        # FD 3 will be our token stream
        exec 3<>"$fifo"
        rm -f "$fifo"   # fifo persists via FD

        # seed tokens
        for _ in $(seq 1 "$PARALLEL_JOBS"); do
          printf '.' >&3
        done

        # ensure FD is closed at the end so the final read unblocks
        stack_trap 'exec 3>&- 3<&- || true' EXIT

        local job_num=0
        for db in "${DBS[@]}"; do
          # acquire a token (blocks if pool is empty)
          read -r -u 3 _
          ((job_num++))

          {
            # run one dump
            local status_file="$status_dir/job_${job_num}.status"
            if dump_one "$db"; then
              echo "ok" > "$status_file"
            else
              echo "error" > "$status_file"
            fi
          } &
          
          # watcher that waits for the job above and returns token
          {
            local pid=$!
            wait "$pid" 2>/dev/null || true
            # return the token
            printf '.' >&3
          } &
        done

        # Drain: take all tokens back so we know all jobs finished.
        for _ in $(seq 1 "$PARALLEL_JOBS"); do
          read -r -u 3 _
        done

        # close semaphore FD (also done by trap)
        exec 3>&- 3<&- || true
        stop_spinner
        
        # Count results from status files
        for sf in "$status_dir"/job_*.status; do
            [[ -f "$sf" ]] || continue
            if grep -q "ok" "$sf" 2>/dev/null; then
                ((completed++))
            else
                ((errors++))
            fi
        done
        
        # Show final progress
        show_progress "$completed" "${#DBS[@]}" "Backup"
    else
        for db in "${DBS[@]}"; do
            if dump_one "$db"; then
                ((completed++))
            else
                ((errors++))
            fi
            show_progress "$completed" "${#DBS[@]}" "Backup"
        done
    fi

    # Copy binlog index
    [[ -r /var/lib/mysql/binlog.index ]] && cp -f /var/lib/mysql/binlog.index "$BACKUP_DIR/binlog-index-$ts.txt" || true

    if (( errors > 0 )); then
        warn "Backup completed with $errors error(s)"
        add_summary "Full backup: $completed OK, $errors failed"
        notify "Backup completed with errors" "Completed: $completed, Failed: $errors" "error"
        return 1
    fi

    log INFO "✅ Full backup complete: $completed databases"
    add_summary "Full backup: $completed databases backed up successfully"
    notify "Backup completed successfully" "Backed up $completed databases" "info"
}

backup_incremental() {
    local last_meta="$(ls -1t "$BACKUP_DIR"/backup-*.meta 2>/dev/null | head -1)"
    
    if [[ -z "$last_meta" ]]; then
        warn "No previous full backup found, performing full backup instead"
        backup_full
        return
    fi
    
    # shellcheck disable=SC1090
    source "$last_meta"
    
    if [[ -z "${binlog_file:-}" ]]; then
        err "Last backup metadata incomplete, cannot perform incremental backup"
    fi
    
    local ts="$(date +'%Y-%m-%d-%H-%M-%S')"
    local comp="$(compressor)"
    local ext="$(get_extension)"
    
    log INFO "Starting incremental backup from $binlog_file:$binlog_pos"
    
    local output="$BACKUP_DIR/incremental-${ts}.binlog.${ext}"
    
    # Adjust extension if encryption is on
    [[ "$ENCRYPT_BACKUPS" == "1" ]] && output="${output}.enc"

    if "$MYSQLBINLOG" --start-position="$binlog_pos" "/var/lib/mysql/${binlog_file}"* \
      | $comp | encrypt_if_enabled > "$output"; then
      create_checksum "$output"
      log INFO "✅ Incremental backup created: $(basename "$output")"
      add_summary "Incremental backup: $(basename "$output")"
    else
      err "Incremental backup failed"
    fi

}

cleanup_old_backups() {
  # Age-based pruning for dumps, binlog bundles, and checksums
  [[ -d "$BACKUP_DIR" ]] || { log INFO "Backup dir not found: $BACKUP_DIR"; return 0; }

  log INFO "Pruning items older than ${RETENTION_DAYS} day(s)… (dry_run=$DRY_RUN)"
  local f
  if (( DRY_RUN == 1 )); then
    while IFS= read -r f; do
      log INFO "DRY-RUN: Would delete $(basename "$f")"
    done < <(find "$BACKUP_DIR" -maxdepth 1 -type f \
              \( -name "*.sql.*" -o -name "*.binlog.*" -o -name "*.sha256" \) \
              -mtime +"${RETENTION_DAYS}" -print 2>/dev/null)
  else
    # Print first for visibility, then delete
    find "$BACKUP_DIR" -maxdepth 1 -type f \
         \( -name "*.sql.*" -o -name "*.binlog.*" -o -name "*.sha256" \) \
         -mtime +"${RETENTION_DAYS}" -print -delete 2>/dev/null || true
  fi
}



# ========================== Verify Functions ==========================

verify() {
    require_login
    shopt -s nullglob
    
    local files=("$BACKUP_DIR"/*.sql.* "$BACKUP_DIR"/*.binlog.*)
    [[ ${#files[@]} -gt 0 ]] || { log INFO "No backups found in $BACKUP_DIR"; return 0; }
    
    log INFO "Verifying ${#files[@]} backup file(s)..."
    
    local ok=0 bad=0 warn_count=0
    local idx=0
    
    start_spinner "Verifying backups"

    for f in "${files[@]}"; do
        ((idx++))
        show_progress "$idx" "${#files[@]}" "Verify"
        
        local base="$(basename "$f")"
        
        # Skip checksum and metadata files
        [[ "$base" =~ \.(sha256|meta)$ ]] && continue
        
        # Verify checksum first
        if ! verify_checksum "$f"; then
            ((warn_count++))
        fi
        
        # Verify compression/encryption integrity
        local decomp_cmd="$(decompressor "$f")"
        if [[ "$f" =~ \.enc$ ]]; then
          # decrypt → decompress to /dev/null (use stdin, not filename)
          if decrypt_if_encrypted "$f" < "$f" | $decomp_cmd > /dev/null 2>&1; then
            ((ok++)); debug "OK(enc): $base"
          else
            ((bad++)); warn "CORRUPT (enc): $base"; continue
          fi
        else
          # direct decompress to /dev/null (use stdin, not filename)
          if $decomp_cmd < "$f" > /dev/null 2>&1; then
            ((ok++)); debug "OK: $base"
          else
            ((bad++)); warn "CORRUPT: $base"; continue
          fi
        fi
        
        # Check for metadata
        local ts="$(dump_ts_from_name "$base")"
        if [[ -n "$ts" ]]; then
            local meta="$BACKUP_DIR/backup-$ts.meta"
            if [[ ! -f "$meta" ]]; then
                ((warn_count++))
                debug "Missing metadata: $base"
            fi
        fi
    done
    
    stop_spinner
    log INFO "Verify complete: OK=$ok, Corrupt=$bad, Warnings=$warn_count"
    add_summary "Verification: $ok OK, $bad corrupt, $warn_count warnings"
    
    if (( bad > 0 )); then
        notify "Backup verification failed" "$bad corrupt file(s) found" "error"
        return 1
    fi
    
    [[ "$ok" -gt 0 ]] && notify "Backup verification passed" "$ok file(s) verified successfully" "info"
}

# ========================== Restore Functions ==========================

restore() {
    need_tooling
    require_login
    
    local arg1="${1:-}"
    local target_db_opt="${2:-}"
    local until_time="${UNTIL_TIME:-}"
    local end_pos="${END_POS:-}"
    
    # Validate datetime format if provided
    if [[ -n "$until_time" ]]; then
        if ! date -d "$until_time" >/dev/null 2>&1; then
            err "Invalid UNTIL_TIME format: '$until_time' (expected: YYYY-MM-DD HH:MM:SS)"
        fi
        log INFO "PITR target time: $until_time"
    fi
    
    # Validate end position if provided
    if [[ -n "$end_pos" ]]; then
        if ! [[ "$end_pos" =~ ^[0-9]+$ ]]; then
            err "Invalid END_POS: '$end_pos' (must be a positive integer)"
        fi
        log INFO "PITR end position: $end_pos"
    fi
    
    if [[ -z "$arg1" ]]; then
        cat <<EOF
Usage:
  $0 restore <DB|ALL>
  $0 restore /path/to/backup.tar.gz [target-db]
  
Optional PITR environment variables:
  UNTIL_TIME='YYYY-MM-DD HH:MM:SS'  - Replay binlogs until this timestamp
  END_POS=<position>                - Stop at this binlog position
  
Examples:
  # Basic restore (auto-detects backup type)
  $0 restore mydb
  
  # Restore XtraBackup
  $0 restore /var/backups/mysql/xtra-full-2025-01-15-10-00-00.tar.gz
  
  # Restore mysqldump with rename
  $0 restore /path/to/mydb-backup.sql.gz test_database
  
  # Point-in-time restore
  UNTIL_TIME='2025-01-15 10:30:00' $0 restore mydb
EOF
        exit 1
    fi
    
    # Handle file-based restore
    if [[ -f "$arg1" ]]; then
        # Auto-detect backup type
        if [[ "$arg1" =~ xtra-(full|incr) ]]; then
            restore_xtra "$arg1" "$target_db_opt" "$until_time"
        else
            restore_file "$arg1" "$target_db_opt" "$until_time" "$end_pos"
        fi
        return
    fi
    
    # Handle DB name or ALL - find latest backup
    if [[ "$arg1" == "ALL" || ! -f "$arg1" ]]; then
        # Try XtraBackup first
        local xtra_backup=$(find "$BACKUP_DIR" -name "xtra-full-*.tar.*" 2>/dev/null | sort -r | head -1)
        if [[ -n "$xtra_backup" && -n "$XTRABACKUP" ]]; then
            log INFO "Found XtraBackup: $(basename "$xtra_backup")"
            restore_xtra "$xtra_backup" "" "$until_time"
        else
            # Fallback to mysqldump
            restore_database "$arg1" "$until_time" "$end_pos"
        fi
        return
    fi
}

restore_file() {
    local file="$1"
    local target_db_opt="$2"
    local until_time="$3"
    local end_pos="$4"
    
    log INFO "Restoring from file: $(basename "$file")"
    
    # Verify file
    verify_checksum "$file" || warn "Checksum verification failed"
    
    local decomp_cmd="$(decompressor "$file")"
    if [[ "$file" =~ \.enc$ ]]; then
      decrypt_if_encrypted "$file" < "$file" | $decomp_cmd >/dev/null 2>&1 \
        || err "Backup file appears corrupt (enc): $file"
    else
      $decomp_cmd < "$file" >/dev/null 2>&1 \
        || err "Backup file appears corrupt: $file"
    fi
    
    # Extract database name
    local base="$(basename "$file")"
    local src_db="${base%%-[0-9][0-9][0-9][0-9]-*}"
    [[ -n "$src_db" ]] || err "Cannot determine source database from filename"
    
    local dest_db="${target_db_opt:-$src_db}"
    local dump_ts="$(dump_ts_from_name "$base")"
    
    log INFO "Source DB: $src_db → Target DB: $dest_db"
    
    if [[ "${DROP_FIRST:-0}" == "1" ]]; then
        warn "DROP_FIRST=1: Dropping database \`$dest_db\`"
        "$MYSQL" --login-path="$LOGIN_PATH" -e "DROP DATABASE IF EXISTS \`$dest_db\`;"
    fi
    
    "$MYSQL" --login-path="$LOGIN_PATH" -e "CREATE DATABASE IF NOT EXISTS \`$dest_db\` CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;"
    
    if [[ "$dest_db" == "$src_db" ]]; then
        decrypt_if_encrypted "$file" < "$file" | $decomp_cmd | "$MYSQL" --login-path="$LOGIN_PATH"
    else
        warn "Renaming database on restore: $src_db → $dest_db"
        
        # Escape special regex characters in database names
        local src_escaped dest_escaped
        src_escaped=$(printf '%s\n' "$src_db" | sed 's/[.[\*^$]/\\&/g')
        dest_escaped=$(printf '%s\n' "$dest_db" | sed 's/[&/\]/\\&/g')
        
        decrypt_if_encrypted "$file" < "$file" | $decomp_cmd \
            | sed -E \
                -e "s/^(CREATE DATABASE[^;]*\`)${src_escaped}(\`)/\1${dest_escaped}\2/g" \
                -e "s/^USE \`${src_escaped}\`/USE \`${dest_escaped}\`/g" \
            | "$MYSQL" --login-path="$LOGIN_PATH"
    fi
    
    if [[ -n "$until_time" || -n "$end_pos" ]]; then
        run_pitr_local "$dest_db" "$until_time" "$end_pos" "$dump_ts"
    fi
    
    log INFO "✅ Restore complete"
    add_summary "Restored: $dest_db"
}

restore_database() {
    local target="$1"
    local until_time="$2"
    local end_pos="$3"
    
    shopt -s nullglob
    local files=()
    
    if [[ "$target" == "ALL" ]]; then
        files=("$BACKUP_DIR"/*-*.sql.*)
        [[ ${#files[@]} -gt 0 ]] || err "No backups found"
        log INFO "Restoring ALL databases"
    else
        files=($(ls -1t "$BACKUP_DIR"/"$target"-*.sql.* 2>/dev/null | grep -v '\.sha256$' || true))
        [[ ${#files[@]} -gt 0 ]] || err "No backups found for: $target"
        files=("${files[0]}")
        log INFO "Restoring: $target from $(basename "${files[0]}")"
    fi
    
    local chosen_ts=""
    for f in "${files[@]}"; do
        local decomp_cmd="$(decompressor "$f")"
        log INFO "Restoring: $(basename "$f")"
        decrypt_if_encrypted "$f" < "$f" | $decomp_cmd | "$MYSQL" --login-path="$LOGIN_PATH"
        
        if [[ "$target" != "ALL" && -z "$chosen_ts" ]]; then
            chosen_ts="$(dump_ts_from_name "$(basename "$f")")"
        fi
    done
    
    if [[ -n "$until_time" || -n "$end_pos" ]]; then
        if [[ "$target" == "ALL" ]]; then
            run_pitr_local "" "$until_time" "$end_pos"
        else
            run_pitr_local "$target" "$until_time" "$end_pos" "$chosen_ts"
        fi
    fi
    
    log INFO "✅ Restore complete"
    add_summary "Restored: $target"
}

run_pitr_local() {
    local db_filter="$1"
    local until_time="$2"
    local end_pos="$3"
    local dump_ts="${4:-}"
    
    [[ -z "$MYSQLBINLOG" ]] && { warn "mysqlbinlog not available, skipping PITR"; return 0; }
    
    # Validate datetime format if provided
    if [[ -n "$until_time" ]]; then
        if ! date -d "$until_time" >/dev/null 2>&1; then
            err "Invalid UNTIL_TIME format: $until_time (expected: YYYY-MM-DD HH:MM:SS)"
        fi
    fi
    
    local meta
    if [[ -n "$dump_ts" && -f "$BACKUP_DIR/backup-$dump_ts.meta" ]]; then
        meta="$BACKUP_DIR/backup-$dump_ts.meta"
    else
        meta="$(ls -1t "$BACKUP_DIR"/backup-*.meta 2>/dev/null | head -1 || true)"
    fi
    
    if [[ -z "$meta" ]]; then
        warn "No metadata found for PITR"
        return 0
    fi
    
    # shellcheck disable=SC1090
    source "$meta"
    
    if [[ -z "${binlog_file:-}" ]]; then
        warn "No binlog information in metadata"
        return 0
    fi
    
    local index="/var/lib/mysql/binlog.index"
    local files=()
    
    if [[ -r "$index" ]]; then
        while IFS= read -r line; do
            line="${line%\"}"; line="${line#\"}"
            [[ -f "$line" ]] && files+=("$line")
        done < "$index"
    else
        [[ -f "/var/lib/mysql/$binlog_file" ]] && files=("/var/lib/mysql/$binlog_file")
    fi
    
    if [[ ${#files[@]} -eq 0 ]]; then
        warn "No binlog files found for PITR"
        return 0
    fi
    
    local start_idx=-1
    for i in "${!files[@]}"; do
        if [[ "$(basename "${files[$i]}")" == "$binlog_file" ]]; then
            start_idx="$i"
            break
        fi
    done
    
    if [[ "$start_idx" -lt 0 ]]; then
        warn "Starting binlog not found: $binlog_file"
        return 0
    fi
    
    log INFO "Applying PITR from $binlog_file"
    [[ -n "$db_filter" ]] && log INFO "Database filter: $db_filter"
    [[ -n "$until_time" ]] && log INFO "Until time: $until_time"
    [[ -n "$end_pos" ]] && log INFO "End position: $end_pos"
    
    local last_idx=$(( ${#files[@]} - 1 ))
    local stopped_early=0
    
    for i in $(seq "$start_idx" "$last_idx"); do
        local cmd=("$MYSQLBINLOG")
        [[ -n "$db_filter" ]] && cmd+=("--database=$db_filter")
        
        # Apply start position only for the first file
        if [[ "$i" -eq "$start_idx" && -n "${binlog_pos:-}" ]]; then
            cmd+=("--start-position=${binlog_pos}")
        fi
        
        # Always apply stop criteria - mysqlbinlog will stop when reached
        [[ -n "$until_time" ]] && cmd+=("--stop-datetime=$until_time")
        
        # Stop position only applies to the last binlog file
        if [[ "$i" -eq "$last_idx" && -n "$end_pos" ]]; then
            cmd+=("--stop-position=$end_pos")
        fi
        
        cmd+=("${files[$i]}")
        
        log INFO "Processing binlog: $(basename "${files[$i]}")"
        
        if ! "${cmd[@]}" | "$MYSQL" --login-path="$LOGIN_PATH"; then
            # Check if this is expected (reached target time)
            if [[ -n "$until_time" ]]; then
                debug "Binlog replay stopped (likely reached target time)"
                stopped_early=1
                break
            else
                warn "Binlog replay error for ${files[$i]}"
            fi
        fi
        
        # If we have a stop-datetime, check if we should continue
        # (mysqlbinlog may have stopped when it reached the target time)
        if [[ -n "$until_time" && "$i" -lt "$last_idx" ]]; then
            # Check if the next binlog file's first timestamp is after our target
            local next_file="${files[$((i+1))]}"
            if [[ -f "$next_file" ]]; then
                local first_ts
                first_ts=$("$MYSQLBINLOG" --start-position=4 "$next_file" 2>/dev/null | grep -m1 "^#[0-9]" | awk '{print $1 " " $2}' | sed 's/#//' || true)
                if [[ -n "$first_ts" ]]; then
                    # Compare timestamps (basic check)
                    if [[ "$first_ts" > "$until_time" ]]; then
                        log INFO "Next binlog starts after target time, stopping"
                        stopped_early=1
                        break
                    fi
                fi
            fi
        fi
    done
    
    log INFO "✅ PITR replay complete"
}

# ========================== Database Information ==========================

sizes() {
    require_login
    
    log INFO "Database sizes:"
    "$MYSQL" --login-path="$LOGIN_PATH" -e "
        SELECT table_schema AS db,
               ROUND(SUM(data_length+index_length)/1024/1024,2) AS size_mb
        FROM information_schema.TABLES
        WHERE table_schema NOT IN ('mysql','information_schema','performance_schema','sys')
        GROUP BY table_schema
        ORDER BY size_mb DESC;"
    
    echo
    log INFO "Top $TOP_N_TABLES tables by size:"
    "$MYSQL" --login-path="$LOGIN_PATH" -e "
        SELECT CONCAT(table_schema,'.',table_name) AS table_name,
               ROUND((data_length+index_length)/1024/1024,2) AS size_mb,
               ENGINE
        FROM information_schema.TABLES
        WHERE table_schema NOT IN ('mysql','information_schema','performance_schema','sys')
        ORDER BY size_mb DESC
        LIMIT ${TOP_N_TABLES};"
}

# ========================== Health Check ==========================

health() {
    log INFO "=== DB Tools Health Check ==="
    echo
    
    local status=0
    
    # MySQL connectivity
    if "$MYSQL" --login-path="$LOGIN_PATH" -e "SELECT 1;" >/dev/null 2>&1; then
        echo "✅ MySQL Connection: OK"
    else
        echo "❌ MySQL Connection: FAILED"
        ((status++))
    fi
    
    # Backup directory
    if [[ -d "$BACKUP_DIR" && -w "$BACKUP_DIR" ]]; then
        echo "✅ Backup Directory: OK ($BACKUP_DIR)"
    else
        echo "❌ Backup Directory: Not writable"
        ((status++))
    fi
    
    # Disk space - use MB for precision
    local available_mb
    available_mb=$(df -BM "$BACKUP_DIR" 2>/dev/null | awk 'NR==2 {print $4}' | sed 's/M//' || echo "0")
    local available_gb=$((available_mb / 1024))
    
    if (( available_gb > 10 )); then
        echo "✅ Disk Space: ${available_gb}GB available (${available_mb}MB)"
    elif (( available_gb > 5 )); then
        echo "⚠️  Disk Space: ${available_gb}GB available (getting low)"
        ((status++))
    else
        echo "❌ Disk Space: ${available_gb}GB available (critically low!)"
        ((status++))
    fi
    
    # Last backup
    local last_backup
    last_backup=$(find "$BACKUP_DIR" -name "*.sql.*" -type f ! -name "*.sha256" -printf '%T@ %p\n' 2>/dev/null | sort -n | tail -1 | cut -d' ' -f2- || true)
    if [[ -n "$last_backup" ]]; then
        local age=$(( ($(date +%s) - $(stat -c %Y "$last_backup" 2>/dev/null || echo 0)) / 3600 ))
        if (( age < 48 )); then
            echo "✅ Last Backup: ${age}h ago ($(basename "$last_backup"))"
        else
            echo "⚠️  Last Backup: ${age}h ago (outdated!)"
            ((status++))
        fi
    else
        echo "⚠️  Last Backup: No backups found"
        ((status++))
    fi
    
    # Binary logging
    local binlog_status
    binlog_status=$("$MYSQL" --login-path="$LOGIN_PATH" -N -e "SHOW VARIABLES LIKE 'log_bin';" 2>/dev/null | awk '{print $2}' || echo "OFF")
    if [[ "$binlog_status" == "ON" ]]; then
        echo "✅ Binary Logging: Enabled (PITR available)"
    else
        echo "⚠️  Binary Logging: Disabled (PITR unavailable)"
    fi
    
    # Compression tool
    if have pigz || have zstd; then
        echo "✅ Compression: Parallel compression available ($COMPRESS_ALGO)"
    else
        echo "⚠️  Compression: Using gzip (slower)"
    fi

    # XtraBackup
    if [[ -n "$XTRABACKUP" ]]; then
        local xtra_version
        xtra_version=$("$XTRABACKUP" --version 2>&1 | head -1 | awk '{print $3}' || echo "unknown")
        echo "✅ XtraBackup: Installed ($xtra_version)"
    else
        echo "⚠️  XtraBackup: Not installed (physical backups unavailable)"
    fi    
    
    # Checksums
    if [[ "$CHECKSUM_ENABLED" == "1" ]] && have sha256sum; then
        echo "✅ Checksums: Enabled"
    else
        echo "⚠️  Checksums: Disabled or sha256sum not available"
    fi
    
    # Encryption
    if [[ "$ENCRYPT_BACKUPS" == "1" ]] && [[ -f "$ENCRYPTION_KEY_FILE" ]]; then
        echo "✅ Encryption: Enabled"
        check_key_perms || ((status++))
    else
        echo "ℹ️  Encryption: Disabled"
    fi
    
    # Notifications
    if [[ -n "$NOTIFY_EMAIL" ]] || [[ -n "$NOTIFY_WEBHOOK" ]]; then
        echo "✅ Notifications: Configured"
    else
        echo "ℹ️  Notifications: Not configured"
    fi
    
    echo
    if (( status == 0 )); then
        log INFO "✅ All health checks passed"
    else
        warn "$status issue(s) detected"
    fi
    
    return "$status"
}

# ========================== Tuning ==========================

tune() {
    require_login
    
    if have mysqltuner; then
        log INFO "Running MySQLTuner..."
        mysqltuner --silent --forcemem --nocolor 2>/dev/null || true
    else
        warn "mysqltuner not found (install: apt-get install mysqltuner)"
    fi
    
    echo
    
    if have pt-variable-advisor; then
        log INFO "Running Percona pt-variable-advisor..."
        "$MYSQL" --login-path="$LOGIN_PATH" -e "SHOW VARIABLES" | pt-variable-advisor --quiet - || true
    else
        warn "pt-variable-advisor not found (install percona-toolkit)"
    fi
}

# ========================== Maintenance ==========================

maintain() {
    require_login
    acquire_lock
    
    local mode="${1:-quick}"
    
    case "$mode" in
        quick|full) ;;
        *) err "Mode must be 'quick' or 'full'" ;;
    esac
    
    log INFO "Running maintenance mode: $mode"
    
    mapfile -t tables < <(
        "$MYSQL" --login-path="$LOGIN_PATH" -N -e "
            SELECT CONCAT(TABLE_SCHEMA,'.',TABLE_NAME)
            FROM information_schema.TABLES
            WHERE TABLE_SCHEMA NOT IN ('mysql','information_schema','performance_schema','sys')
              AND TABLE_TYPE='BASE TABLE'
            ORDER BY TABLE_SCHEMA, TABLE_NAME;"
    )
    
    [[ ${#tables[@]} -gt 0 ]] || { log INFO "No tables found"; return 0; }
    
    log INFO "Analyzing ${#tables[@]} table(s)..."
    local idx=0
    
    for t in "${tables[@]}"; do
        ((idx++))
        show_progress "$idx" "${#tables[@]}" "ANALYZE"
        "$MYSQL" --login-path="$LOGIN_PATH" -e "ANALYZE TABLE ${t};" >/dev/null 2>&1 || warn "Failed: $t"
    done
    
    if [[ "$mode" == "full" ]]; then
        warn "OPTIMIZE mode: This may take a long time and require disk space"
        idx=0
        
        for t in "${tables[@]}"; do
            ((idx++))
            show_progress "$idx" "${#tables[@]}" "OPTIMIZE"
            "$MYSQL" --login-path="$LOGIN_PATH" -e "OPTIMIZE TABLE ${t};" >/dev/null 2>&1 || warn "Failed: $t"
        done
    fi
    
    log INFO "✅ Maintenance complete"
    add_summary "Maintenance ($mode): ${#tables[@]} tables processed"
}

# ========================== Cleanup ==========================

cleanup() {
  local days="${1:-$RETENTION_DAYS}"
  local deleted_count=0
  [[ -d "$BACKUP_DIR" ]] || { log INFO "Backup dir not found: $BACKUP_DIR"; return 0; }

  log INFO "Starting cleanup (days=$days, keep_min=$CLEAN_KEEP_MIN, dry_run=$DRY_RUN)"
  shopt -s nullglob

  # ---- Per-DB pruning for full dumps (*.sql.*) with minimum keep ----
  declare -A bydb=()
  local f base db
  for f in "$BACKUP_DIR"/*-*.sql.*; do
    [[ -f "$f" ]] || continue
    [[ "$f" =~ \.(sha256|meta)$ ]] && continue
    base="$(basename "$f")"
    db="${base%%-[0-9][0-9][0-9][0-9]-*}"
    bydb["$db"]+="$f"$'\n'
  done

  local -a files
  local keep_count
  for db in "${!bydb[@]}"; do
    # Build array of files sorted by modification time (newest first)
    files=()
    while IFS= read -r line; do
        [[ -n "$line" ]] && files+=("$line")
    done < <(printf "%s" "${bydb[$db]}" | sed '/^$/d' | xargs -d '\n' -I{} stat -c "%Y {}" {} 2>/dev/null | sort -rn | cut -d' ' -f2-)
    
    keep_count=0
    for f in "${files[@]}"; do
      if (( keep_count < CLEAN_KEEP_MIN )); then
        ((keep_count++))
        debug "Keeping (min): $(basename "$f")"
        continue
      fi
      # Check if file is older than retention period
      if [[ -n "$(find "$f" -mtime +"$days" -print -quit 2>/dev/null)" ]]; then
        if (( DRY_RUN == 1 )); then
          log INFO "DRY-RUN: Would delete $(basename "$f")"
          ((deleted_count++))
        else
          log INFO "Deleting: $(basename "$f")"
          rm -f "$f" "${f}.sha256" 2>/dev/null || true
          ((deleted_count++))
        fi
      fi
    done
  done

  # ---- Age-based pruning for incremental binlog bundles (*.binlog.*) ----
  while IFS= read -r f; do
    [[ -f "$f" ]] || continue
    if (( DRY_RUN == 1 )); then
      log INFO "DRY-RUN: Would delete $(basename "$f")"
      ((deleted_count++))
    else
      log INFO "Deleting: $(basename "$f")"
      rm -f "$f" "${f}.sha256" 2>/dev/null || true
      ((deleted_count++))
    fi
  done < <(find "$BACKUP_DIR" -maxdepth 1 -type f -name "incremental-*.binlog.*" -mtime +"$days" -print 2>/dev/null)

  # ---- Orphaned metadata (backup-*.meta with no matching dump) ----
  local m ts
  for m in "$BACKUP_DIR"/backup-*.meta; do
    [[ -e "$m" ]] || break
    ts="${m##*/}"; ts="${ts#backup-}"; ts="${ts%.meta}"
    if ! compgen -G "$BACKUP_DIR/*-$ts.sql.*" >/dev/null 2>&1; then
      if (( DRY_RUN == 1 )); then
        log INFO "DRY-RUN: Would delete orphan meta $(basename "$m")"
        ((deleted_count++))
      else
        log INFO "Deleting orphan meta: $(basename "$m")"
        rm -f "$m"
        ((deleted_count++))
      fi
    fi
  done

  # ---- Old captured binlog index snapshots ----
  local idxf
  while IFS= read -r idxf; do
    if (( DRY_RUN == 1 )); then
      log INFO "DRY-RUN: Would delete $(basename "$idxf")"
      ((deleted_count++))
    else
      log INFO "Deleting: $(basename "$idxf")"
      rm -f "$idxf"
      ((deleted_count++))
    fi
  done < <(find "$BACKUP_DIR" -maxdepth 1 -type f -name "binlog-index-*.txt" -mtime +"$days" -print 2>/dev/null)

  log INFO "✅ Cleanup complete"
  add_summary "Cleanup: $deleted_count file(s) removed (retention: $days days, keep_min: $CLEAN_KEEP_MIN)"
}


# ========================== List Backups ==========================

list_backups() {
  shopt -s nullglob
  local files=("$BACKUP_DIR"/*.sql.* "$BACKUP_DIR"/*.binlog.*)
  [[ ${#files[@]} -gt 0 ]] || { log INFO "No backups found"; return 0; }

  echo "Available backups in $BACKUP_DIR:"
  echo
  printf "%-50s %-16s %-10s\n" "FILE" "DATE" "SIZE"
  printf "%s\n" "$(printf '=%.0s' {1..80})"

  local f name size rawdate showdate
  for f in "${files[@]}"; do
    [[ "$f" =~ \.sha256$ ]] && continue
    name="$(basename "$f")"
    size=$(du -h "$f" 2>/dev/null | cut -f1 || echo "?")
    if rawdate=$(stat -c %y "$f" 2>/dev/null | cut -d'.' -f1); then
      showdate=$(date -d "$rawdate" '+%Y-%m-%d %H:%M' 2>/dev/null || echo "$rawdate")
    else
      # BSD stat fallback
      rawdate=$(stat -f "%Sm" -t "%Y-%m-%d %H:%M" "$f" 2>/dev/null || echo "unknown")
      showdate="$rawdate"
    fi
    printf "%-50s %-16s %-10s\n" "$name" "$showdate" "$size"
  done
}


# ========================== Generate Config ==========================

generate_config() {
    local config_path="${1:-$CONFIG_FILE}"
    
    if [[ -f "$config_path" ]]; then
        read -r -p "Config file exists. Overwrite? [y/N]: " confirm
        [[ "$confirm" == "y" || "$confirm" == "Y" ]] || { log INFO "Cancelled"; return 0; }
    fi
    
    cat > "$config_path" <<'EOF'
# db-tools configuration file
# Source this file or set as CONFIG_FILE environment variable

# Backup settings
BACKUP_DIR="/var/backups/mysql"
RETENTION_DAYS=7
PARALLEL_JOBS=2

# Compression (pigz, gzip, zstd, xz)
COMPRESS_ALGO="pigz"
COMPRESS_LEVEL=6

# XtraBackup (physical backups - faster for InnoDB)
BACKUP_METHOD="xtrabackup"  # xtrabackup or mysqldump
XTRABACKUP_ENABLED=1
XTRABACKUP_PARALLEL=4
XTRABACKUP_COMPRESS=1
XTRABACKUP_COMPRESS_THREADS=4
XTRABACKUP_MEMORY="1G"  # Memory for prepare phase

# GFS Rotation
KEEP_DAILY=7
KEEP_WEEKLY=4
KEEP_MONTHLY=6
CLEAN_KEEP_MIN=2

# Encryption (0=disabled, 1=enabled)
ENCRYPT_BACKUPS=0
ENCRYPTION_KEY_FILE="/etc/db-tools-encryption.key"

# Notifications
NOTIFY_EMAIL=""
NOTIFY_WEBHOOK=""
NOTIFY_ON="error"  # always, error, never

# Logging
LOG_LEVEL="INFO"  # DEBUG, INFO, WARN, ERROR
USE_SYSLOG=0

# Features
CHECKSUM_ENABLED=1
DROP_FIRST=0
DRY_RUN=0

# MySQL
LOGIN_PATH="dbtools"
TOP_N_TABLES=30
EOF
    
    log INFO "✅ Config file created: $config_path"
    log INFO "Edit the file and reload with: export CONFIG_FILE=$config_path"
}

# ========================== Usage/Help ==========================

usage() {
    cat <<EOF
db-tools - MySQL/MariaDB Administration Toolkit

Usage: $0 <command> [options]

Commands:
  init                          Initialize login credentials and tools
  backup [full|incremental]     Create backup (XtraBackup by default)
  backup logical                Create logical backup (mysqldump)
  verify                        Verify backup integrity with checksums
  restore <DB|ALL|file>         Restore (auto-detects backup type)
  list                          List all available backups
  
  health                        Run system health check
  sizes                         Show database and table sizes
  tune                          Run tuning advisors (mysqltuner, pt-variable-advisor)
  maintain [quick|full]         Run ANALYZE (quick) or OPTIMIZE (full)
  cleanup [days]                Remove old backups (default: RETENTION_DAYS)
  
  config [path]                 Generate sample configuration file
  genkey [path]                 Generate encryption key file
  help                          Show this help message

Environment Variables:
  BACKUP_DIR                    Backup storage location (default: /var/backups/mysql)
  RETENTION_DAYS                Days to keep backups (default: 7)
  LOGIN_PATH                    MySQL login path (default: dbtools)
  PARALLEL_JOBS                 Parallel backup jobs (default: 2)
  COMPRESS_ALGO                 pigz, gzip, zstd, xz (default: pigz)
  ENCRYPT_BACKUPS               0=off, 1=on (default: 0)
  CHECKSUM_ENABLED              0=off, 1=on (default: 1)
  NOTIFY_EMAIL                  Email for notifications
  NOTIFY_WEBHOOK                Webhook URL for notifications
  LOG_LEVEL                     DEBUG, INFO, WARN, ERROR (default: INFO)
  DRY_RUN                       0=off, 1=on (default: 0)
  
PITR (Point-in-Time Recovery):
  UNTIL_TIME="YYYY-MM-DD HH:MM:SS"
  END_POS=<position>
  
Examples:
  # Initial setup
  $0 init
  
  # Create full backup
  $0 backup
  
  # Restore specific database
  $0 restore mydb
  
  # Point-in-time restore
  UNTIL_TIME="2025-01-15 10:30:00" $0 restore mydb
  
  # Restore with rename
  $0 restore /path/backup.sql.gz new_database_name
  
  # Health check
  $0 health
  
  # Cleanup old backups
  $0 cleanup 30

Configuration:
  Create a config file: $0 config /etc/db-tools.conf
  Load config: export CONFIG_FILE=/etc/db-tools.conf

Documentation:
  For more information, visit: https://github.com/deforay/utility-scripts

Version: 3.0.0
EOF
}

# ========================== Main Dispatcher ==========================

# Load configuration
load_config

# Create directories
mkdir -p "$BACKUP_DIR" "$MARK_DIR" 2>/dev/null || true

# Parse command
cmd="${1:-}"
shift || true

case "$cmd" in
    init)
        init "$@"
        ;;
    backup)
        backup "${1:-full}"
        ;;
    verify)
        verify "$@"
        ;;
    restore)
        restore "$@"
        ;;
    list)
        list_backups "$@"
        ;;
    health)
        health "$@"
        ;;
    sizes)
        sizes "$@"
        ;;
    tune)
        tune "$@"
        ;;
    maintain)
        maintain "${1:-quick}"
        ;;
    cleanup)
        cleanup "${1:-$RETENTION_DAYS}"
        ;;
    config)
        generate_config "${1:-$CONFIG_FILE}"
        ;;
    genkey|generate-key)
        generate_encryption_key "${1:-$ENCRYPTION_KEY_FILE}"
        ;;
    help|-h|--help|"")
        usage
        ;;
    *)
        err "Unknown command: $cmd (use '$0 help' for usage)"
        ;;
esac

exit 0