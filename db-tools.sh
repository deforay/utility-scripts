#!/usr/bin/env bash
#===============================================================================
#
#   db-tools.sh — MySQL/MariaDB Administration Toolkit
#   Version: 3.2.2
#
#   A comprehensive backup, restore, and maintenance solution for MySQL/MariaDB
#   with support for XtraBackup, point-in-time recovery, encryption, and more.
#
#===============================================================================
#
#   FEATURES
#   --------
#   • Physical backups via XtraBackup/MariaBackup (fast, hot backups)
#   • Logical backups via mysqldump (portable, cross-version compatible)
#   • Incremental backups with automatic base detection
#   • Point-in-Time Recovery (PITR) via binary log replay
#   • AES-256-GCM encryption for backup files
#   • Parallel compression (pigz, zstd, xz, gzip)
#   • SHA256 checksums for integrity verification
#   • Email and webhook notifications
#   • Smart space management:
#     - Auto-cleanup old backups when space is low
#     - Size-based retention (max total backup size)
#     - Disk usage warnings at configurable thresholds
#     - Automatic cleanup of orphaned partial files
#     - Minimum free space enforcement
#   • Database health checks and tuning advisors
#   • Safe mode: auto-detects risky conditions (low disk, peak hours)
#
#===============================================================================
#
#   INSTALLATION
#   ------------
#   # One-line install:
#   sudo curl -fsSL "https://raw.githubusercontent.com/deforay/utility-scripts/master/db-tools.sh" -o /usr/local/bin/db-tools && sudo chmod +x /usr/local/bin/db-tools
#
#   # Or clone the repository:
#   git clone https://github.com/deforay/utility-scripts.git
#   sudo cp utility-scripts/db-tools.sh /usr/local/bin/db-tools
#   sudo chmod +x /usr/local/bin/db-tools
#
#===============================================================================
#
#   QUICK START
#   -----------
#   1. Initialize (first-time setup - stores credentials securely):
#      $ sudo db-tools init
#
#   2. Run a backup:
#      $ sudo db-tools backup
#
#   3. Check backup health:
#      $ sudo db-tools health
#
#   4. Set up automated backups (add to root's crontab):
#      $ sudo crontab -e
#
#      # Daily full backup at 2 AM
#      0 2 * * * /usr/local/bin/db-tools backup full >> /var/log/db-tools/backup.log 2>&1
#
#      # Incremental every 6 hours
#      0 */6 * * * /usr/local/bin/db-tools backup incremental >> /var/log/db-tools/backup.log 2>&1
#
#      # Weekly maintenance on Sunday at 3 AM
#      0 3 * * 0 /usr/local/bin/db-tools maintain full >> /var/log/db-tools/maintain.log 2>&1
#
#      # Daily cleanup at 4 AM
#      0 4 * * * /usr/local/bin/db-tools cleanup >> /var/log/db-tools/cleanup.log 2>&1
#
#===============================================================================
#
#   COMMANDS
#   --------
#   init                        Initialize credentials and install dependencies
#   backup [full|incremental]   Create backup (XtraBackup by default)
#   backup logical              Create logical backup (mysqldump)
#   restore <DB|ALL|file>       Restore from backup (auto-detects type)
#   verify                      Verify backup integrity
#   list                        List available backups
#   health                      Run system health check
#   sizes                       Show database and table sizes
#   tune                        Run tuning advisors
#   maintain [quick|full]       ANALYZE (quick) or OPTIMIZE (full) tables
#   cleanup [days]              Remove old backups
#   config [path]               Generate sample config file
#   genkey [path]               Generate encryption key
#   status                      Show current operation status
#   help                        Show help message
#
#===============================================================================
#
#   CONFIGURATION
#   -------------
#   Configuration can be set via:
#   1. Environment variables (highest priority)
#   2. Config file: /etc/db-tools.conf
#   3. Local .env file in current directory
#
#   Generate a sample config:
#   $ sudo db-tools config /etc/db-tools.conf
#
#   Key Settings:
#   -------------
#   BACKUP_DIR          Backup storage location (default: /var/backups/mysql)
#   RETENTION_DAYS      Days to keep backups (default: 7)
#   BACKUP_METHOD       "xtrabackup" or "mysqldump" (default: xtrabackup)
#   COMPRESS_ALGO       zstd, pigz, gzip, xz (default: zstd)
#   PARALLEL_JOBS       Parallel backup jobs (default: auto-detected)
#   LOGIN_PATH          MySQL login path name (default: dbtools)
#
#   Encryption:
#   -----------
#   ENCRYPT_BACKUPS     0=off, 1=on (default: 0)
#   ENCRYPTION_KEY_FILE Path to encryption key (default: /etc/db-tools-encryption.key)
#
#   # Generate encryption key:
#   $ sudo db-tools genkey /etc/db-tools-encryption.key
#
#   Notifications:
#   --------------
#   NOTIFY_EMAIL        Email address for notifications
#   NOTIFY_WEBHOOK      Webhook URL (receives JSON POST)
#   NOTIFY_ON           "always", "error", "never" (default: error)
#
#   Logging:
#   --------
#   LOG_LEVEL           DEBUG, INFO, WARN, ERROR (default: INFO)
#   USE_SYSLOG          0=off, 1=on (default: 0)
#
#===============================================================================
#
#   RESTORE EXAMPLES
#   ----------------
#   # Restore latest backup of a specific database:
#   $ sudo db-tools restore mydb
#
#   # Restore all databases:
#   $ sudo db-tools restore ALL
#
#   # Restore from a specific backup file:
#   $ sudo db-tools restore /var/backups/mysql/mydb-2025-01-15-10-00-00.sql.gz
#
#   # Restore with database rename:
#   $ sudo db-tools restore /var/backups/mysql/mydb-backup.sql.gz new_database_name
#
#   # Restore XtraBackup (auto-detected):
#   $ sudo db-tools restore /var/backups/mysql/xtra-full-2025-01-15-10-00-00.tar
#
#   Point-in-Time Recovery (PITR):
#   ------------------------------
#   # Restore to a specific point in time:
#   $ UNTIL_TIME="2025-01-15 10:30:00" sudo db-tools restore mydb
#
#   # Restore to a specific binlog position:
#   $ END_POS=12345 sudo db-tools restore mydb
#
#   Safety Options:
#   ---------------
#   $ DROP_FIRST=1 sudo db-tools restore mydb    # Drop existing DB first
#   $ FORCE_RESTORE=1 sudo db-tools restore mydb # Skip confirmation prompt
#
#===============================================================================
#
#   XTRABACKUP VS MYSQLDUMP
#   -----------------------
#   XtraBackup (default):
#   • Hot backup - no table locking for InnoDB
#   • Faster for large databases
#   • Supports incremental backups
#   • Requires same MySQL version for restore
#
#   mysqldump:
#   • Portable across MySQL versions
#   • Human-readable SQL output
#   • Better for smaller databases
#   • Can restore individual tables
#
#   To use mysqldump instead:
#   $ BACKUP_METHOD=mysqldump sudo db-tools backup
#   # Or set in /etc/db-tools.conf:
#   BACKUP_METHOD="mysqldump"
#
#===============================================================================
#
#   MAINTENANCE
#   -----------
#   Quick maintenance (ANALYZE only - safe, fast):
#   $ sudo db-tools maintain quick
#
#   Full maintenance (ANALYZE + OPTIMIZE - reclaims disk space):
#   $ sudo db-tools maintain full
#
#   Safe Mode:
#   ----------
#   The script auto-detects risky conditions and enables safe mode:
#   • Low disk space (< 10GB or < 2x largest table)
#   • Peak hours (8 AM - 8 PM by default)
#   • High server load (Threads_running > 25)
#   • Replication lag (> 120 seconds)
#
#   In safe mode, OPTIMIZE is skipped to prevent issues.
#
#   Override safe mode:
#   $ sudo db-tools maintain full --force
#
#   Force safe mode:
#   $ sudo db-tools maintain full --safe
#
#===============================================================================
#
#   SPACE MANAGEMENT
#   ----------------
#   The script includes smart disk space management:
#
#   View space usage:
#   $ sudo db-tools space
#
#   Auto-Cleanup Before Backup:
#   ---------------------------
#   When SPACE_AUTO_CLEANUP=1 (default), the script automatically removes
#   old backups if there isn't enough space for a new backup. It respects
#   CLEAN_KEEP_MIN to always keep a minimum number of backups per database.
#
#   Size-Based Retention:
#   ---------------------
#   Set SPACE_MAX_USAGE_GB to limit total backup storage:
#   $ export SPACE_MAX_USAGE_GB=100  # Max 100GB for backups
#
#   Disk Usage Alerts:
#   ------------------
#   - Warning at SPACE_WARNING_PERCENT (default: 70%)
#   - Critical at SPACE_CRITICAL_PERCENT (default: 90%)
#   - Alerts sent via email/webhook when thresholds exceeded
#
#   Partial File Cleanup:
#   ---------------------
#   Orphaned .partial files from interrupted backups are automatically
#   cleaned up on startup (files older than 1 hour).
#
#   Configuration:
#   --------------
#   SPACE_AUTO_CLEANUP=1        # Enable auto-cleanup (default: 1)
#   SPACE_MAX_USAGE_GB=0        # Max backup size, 0=unlimited (default: 0)
#   SPACE_WARNING_PERCENT=70    # Warning threshold (default: 70)
#   SPACE_CRITICAL_PERCENT=90   # Critical threshold (default: 90)
#   SPACE_MIN_FREE_GB=5         # Minimum free space to keep (default: 5)
#   CLEAN_KEEP_MIN=2            # Minimum backups per DB (default: 2)
#
#===============================================================================
#
#   TROUBLESHOOTING
#   ---------------
#   Check health status:
#   $ sudo db-tools health
#
#   View current operation:
#   $ sudo db-tools status
#
#   Test MySQL connection:
#   $ mysql --login-path=dbtools -e "SELECT 1"
#
#   Re-initialize credentials:
#   $ sudo db-tools init
#
#   Check logs:
#   $ tail -100 /var/log/db-tools/backup.log
#
#   Verify backups:
#   $ sudo db-tools verify
#
#   Common Issues:
#   --------------
#   "Login test failed"
#     → Re-run: sudo db-tools init
#
#   "XtraBackup not found"
#     → Set AUTO_INSTALL=1 or install manually:
#       apt install mariadb-backup  # MariaDB
#       apt install percona-xtrabackup-80  # MySQL 8.0
#
#   "Insufficient disk space"
#     → Free up space or reduce RETENTION_DAYS
#     → Run: sudo db-tools cleanup 3
#
#   "Lock timeout"
#     → Another db-tools instance is running
#     → Check: sudo db-tools status
#     → Remove stale lock: sudo rm /var/run/db-tools.lock
#
#===============================================================================
#
#   SECURITY NOTES
#   --------------
#   • Credentials are stored securely via mysql_config_editor (encrypted)
#   • Backup encryption uses AES-256-GCM (authenticated encryption)
#   • Key files must have 600 permissions and be owned by root
#   • Config files are validated for safe ownership before loading
#   • Database/table names are validated to prevent SQL injection
#
#===============================================================================
#
#   LICENSE
#   -------
#   This script is provided as-is under the MIT License.
#   https://github.com/deforay/utility-scripts
#
#===============================================================================

set -euo pipefail

# Version
DB_TOOLS_VERSION="3.4.0"

# ========================== Configuration ==========================
CONFIG_FILE="${CONFIG_FILE:-/etc/db-tools.conf}"
ENV_FILE="${ENV_FILE:-.env}"
BACKUP_DIR="${BACKUP_DIR:-/var/backups/mysql}"
LOG_DIR="${LOG_DIR:-/var/log/db-tools}"
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

MYSQL_DEFAULTS_FILE="${MYSQL_DEFAULTS_FILE:-/etc/db-tools.my.cnf}"  # for XtraBackup auth


# Compression settings
COMPRESS_ALGO="${COMPRESS_ALGO:-zstd}"  # zstd, pigz, gzip, xz
COMPRESS_LEVEL="${COMPRESS_LEVEL:-6}"

# Backup types and retention
BACKUP_TYPE="${BACKUP_TYPE:-full}"  # full, incremental
KEEP_DAILY="${KEEP_DAILY:-7}"
KEEP_WEEKLY="${KEEP_WEEKLY:-4}"
KEEP_MONTHLY="${KEEP_MONTHLY:-6}"
CLEAN_KEEP_MIN="${CLEAN_KEEP_MIN:-2}"

# Smart space management
SPACE_AUTO_CLEANUP="${SPACE_AUTO_CLEANUP:-1}"       # Auto-remove old backups if space low before backup
SPACE_MAX_USAGE_GB="${SPACE_MAX_USAGE_GB:-0}"       # Max total backup size in GB (0=unlimited)
SPACE_MAX_USAGE_PERCENT="${SPACE_MAX_USAGE_PERCENT:-80}"  # Max % of disk to use for backups
SPACE_WARNING_PERCENT="${SPACE_WARNING_PERCENT:-70}"      # Warn when disk usage exceeds this %
SPACE_CRITICAL_PERCENT="${SPACE_CRITICAL_PERCENT:-90}"    # Critical alert threshold
SPACE_MIN_FREE_GB="${SPACE_MIN_FREE_GB:-5}"         # Minimum free GB to keep on disk
SPACE_CLEANUP_PARTIAL="${SPACE_CLEANUP_PARTIAL:-1}" # Clean orphaned .partial files on startup

# Encryption
ENCRYPT_BACKUPS="${ENCRYPT_BACKUPS:-0}"
ENCRYPTION_KEY_FILE="${ENCRYPTION_KEY_FILE:-}"

# Notifications
NOTIFY_EMAIL="${NOTIFY_EMAIL:-}"
NOTIFY_WEBHOOK="${NOTIFY_WEBHOOK:-}"
NOTIFY_ON="${NOTIFY_ON:-error}"  # always, error, never

# Auto-install behavior
AUTO_INSTALL="${AUTO_INSTALL:-1}"  # Set to 1 to enable auto-installation of tools

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


# ===== Auto-safe heuristics (configurable via env) =====
# Minimum free space (GB) on /var/lib/mysql to allow OPTIMIZE
SAFE_MIN_FREE_GB="${SAFE_MIN_FREE_GB:-10}"
# Free space must also be >= RATIO * largest table size
SAFE_MIN_FREE_RATIO="${SAFE_MIN_FREE_RATIO:-2.0}"
# Consider server "busy" if Threads_running exceeds this
SAFE_MAX_THREADS_RUNNING="${SAFE_MAX_THREADS_RUNNING:-25}"
# Treat these hours (local time) as "peak"; OPTIMIZE avoided unless --force
# Comma-separated 24h hour numbers, e.g. "8-20" = 08:00..20:59
SAFE_PEAK_HOURS="${SAFE_PEAK_HOURS:-8-20}"
# Auto-safe on (1) / off (0)
MAINT_SAFE_AUTO="${MAINT_SAFE_AUTO:-1}"

# --- helpers ---



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


get_free_mb() {
  # Arg: path; prints integer MB free (0 on error)
  local p="${1:-/var/lib/mysql}"
  df -BM "$p" 2>/dev/null | awk 'NR==2{gsub(/M/,"",$4); print int($4)}'
}

get_largest_table_mb() {
  # Uses information_schema; returns MB (integer, 0 if none)
  "$MYSQL" --login-path="$LOGIN_PATH" -N -e "
    SELECT COALESCE(ROUND(MAX((data_length+index_length)/1024/1024)),0)
    FROM information_schema.TABLES
    WHERE table_schema NOT IN ('mysql','information_schema','performance_schema','sys');" 2>/dev/null \
  | awk '{print int($1)}'
}

get_threads_running() {
  "$MYSQL" --login-path="$LOGIN_PATH" -N -e "SHOW GLOBAL STATUS LIKE 'Threads_running';" 2>/dev/null \
    | awk '{print ($2+0)}'
}

replication_lag_seconds() {
  # Works on MySQL/MariaDB; returns seconds or 0 if not a replica / unknown
  local lag=0
  # Try standard SHOW SLAVE/REPLICA STATUS
  local out
  out=$("$MYSQL" --login-path="$LOGIN_PATH" -e "SHOW SLAVE STATUS\G" 2>/dev/null || true)
  if [[ -n "$out" ]]; then
    lag=$(printf "%s\n" "$out" | awk -F': ' '/Seconds_Behind_Master/{print $2+0; exit}')
  else
    out=$("$MYSQL" --login-path="$LOGIN_PATH" -e "SHOW REPLICA STATUS\G" 2>/dev/null || true)
    if [[ -n "$out" ]]; then
      lag=$(printf "%s\n" "$out" | awk -F': ' '/Seconds_Behind_Source/{print $2+0; exit}')
      (( lag == 0 )) && lag=$(printf "%s\n" "$out" | awk -F': ' '/Seconds_Behind_Master/{print $2+0; exit}')
    fi
  fi
  echo $((lag+0))
}

_in_peak_hours() {
  # Parses SAFE_PEAK_HOURS like "8-20" or "9-12,14-18"
  local spec="$SAFE_PEAK_HOURS"
  [[ -z "$spec" ]] && return 1
  local h now
  now=$(date +%H) || now=0
  IFS=',' read -r -a parts <<< "$spec"
  for part in "${parts[@]}"; do
    if [[ "$part" =~ ^([0-9]{1,2})-([0-9]{1,2})$ ]]; then
      local a=${BASH_REMATCH[1]} b=${BASH_REMATCH[2]}
      (( a <= now && now <= b )) && return 0
    elif [[ "$part" =~ ^[0-9]{1,2}$ ]]; then
      (( part == now )) && return 0
    fi
  done
  return 1
}

should_safe_mode() {
  # Returns 0 (true) if we should force safe mode; 1 otherwise
  [[ "$MAINT_SAFE_AUTO" != "1" ]] && return 1

  local free_mb largest_mb threads lag
  free_mb=$(get_free_mb "/var/lib/mysql")
  largest_mb=$(get_largest_table_mb)
  threads=$(get_threads_running || echo 0)
  lag=$(replication_lag_seconds || echo 0)

  # Free space checks
  local min_free_mb=$(( SAFE_MIN_FREE_GB * 1024 ))
  local ratio_need_mb
  # ratio * largest_table (rounded up) - use awk for portable float math
  ratio_need_mb=$(awk -v r="${SAFE_MIN_FREE_RATIO:-2.0}" -v l="${largest_mb:-0}" 'BEGIN { printf "%d", r * l + 0.5 }')
  (( ratio_need_mb == 0 )) && ratio_need_mb=$(( (largest_mb * 2) ))  # fallback

  # Trigger conditions
  if (( free_mb < min_free_mb )); then
    debug "Auto-safe: free_mb($free_mb) < min_free_mb($min_free_mb)"
    return 0
  fi
  if (( free_mb < ratio_need_mb )); then
    debug "Auto-safe: free_mb($free_mb) < ratio_need_mb($ratio_need_mb)"
    return 0
  fi
  if (( threads > SAFE_MAX_THREADS_RUNNING )); then
    debug "Auto-safe: Threads_running($threads) > threshold($SAFE_MAX_THREADS_RUNNING)"
    return 0
  fi
  if _in_peak_hours; then
    debug "Auto-safe: peak hours active ($SAFE_PEAK_HOURS)"
    return 0
  fi
  if (( lag > 120 )); then
    debug "Auto-safe: replication lag ($lag s) > 120 s"
    return 0
  fi

  return 1
}


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
    local width=30

    [[ "$total" -eq 0 ]] && return

    # Cap current at total to prevent overflow
    [[ "$current" -gt "$total" ]] && current="$total"

    local pct=$(( current * 100 / total ))
    local filled=$(( width * current / total ))
    local empty=$(( width - filled ))

    # Build progress bar string
    local bar=""
    for ((i=0; i<filled; i++)); do bar+="█"; done
    for ((i=0; i<empty; i++)); do bar+="░"; done

    if is_tty; then
        # Simple carriage return - no complex cursor movement
        printf '\r%s: [%s] %3d%% (%d/%d)   ' "$desc" "$bar" "$pct" "$current" "$total" >&2
    else
        printf '%s: %d/%d (%d%%)\n' "$desc" "$current" "$total" "$pct" >&2
    fi

    if [[ "$current" -eq "$total" ]] && is_tty; then
        echo >&2
    fi
}

# Create a secure MySQL client defaults file for XtraBackup (non-interactive auth)
write_mysql_defaults_file() {
  local host="$1" port="$2" user="$3" pass="$4"
  local file="${MYSQL_DEFAULTS_FILE:-/etc/db-tools.my.cnf}"

  [[ -z "$host" ]] && host="localhost"
  [[ -z "$port" ]] && port="3306"
  [[ -z "$user" ]] && user="root"

  mkdir -p "$(dirname "$file")" || err "Cannot create $(dirname "$file")"
  # secure file creation
  local old_umask; old_umask=$(umask); umask 077

  cat >"$file" <<EOF
[client]
user = $user
password = $pass
host = $host
port = $port
EOF

  umask "$old_umask"

  chmod 600 "$file" || warn "Could not chmod 600 $file"
  chown root:root "$file" 2>/dev/null || true

  if [[ -s "$file" ]]; then
    log INFO "✅ Wrote XtraBackup credentials to $file (600)"
  else
    err "Failed writing $file"
  fi
}


# Return auth args for XtraBackup, preferring a secure defaults file.
# Echoes a list of CLI tokens, ready to splat into command arrays.
mysql_auth_args() {
  local args=()
  local defaults="${MYSQL_DEFAULTS_FILE:-/etc/db-tools.my.cnf}"

  if [[ -r "$defaults" ]]; then
    args+=("--defaults-file=$defaults")
  else
    local host port user
    host=$(mysql_config_editor print --login-path="$LOGIN_PATH" 2>/dev/null | awk -F= '/host/{gsub(/[ "]/,"",$2);print $2}')
    port=$(mysql_config_editor print --login-path="$LOGIN_PATH" 2>/dev/null | awk -F= '/port/{gsub(/[ "]/,"",$2);print $2}')
    user=$(mysql_config_editor print --login-path="$LOGIN_PATH" 2>/dev/null | awk -F= '/user/{gsub(/[ "]/,"",$2);print $2}')
    [[ -n "$host" ]] && args+=(--host="$host")
    [[ -n "$port" ]] && args+=(--port="$port")
    [[ -n "$user" ]] && args+=(--user="$user")
    # password intentionally not echoed unless using the secure defaults file
  fi

  echo "${args[*]}"
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

# Validate database/table name to prevent SQL injection
# MySQL identifiers: alphanumeric, underscore, dollar sign; max 64 chars
# Reject backticks, quotes, semicolons, and other dangerous chars
validate_identifier() {
    local name="$1"
    local type="${2:-identifier}"  # "database", "table", or "identifier"

    [[ -z "$name" ]] && { warn "Empty $type name"; return 1; }

    # Check length (MySQL max is 64)
    if (( ${#name} > 64 )); then
        warn "Invalid $type name (too long): $name"
        return 1
    fi

    # Allow only safe characters: alphanumeric, underscore, dollar
    # Also allow dot for table.name format
    if [[ ! "$name" =~ ^[a-zA-Z0-9_\$]+(\.[a-zA-Z0-9_\$]+)?$ ]]; then
        warn "Invalid $type name (unsafe characters): $name"
        return 1
    fi

    # Reject reserved/dangerous patterns
    if [[ "$name" =~ [\`\'\"\;\\] ]]; then
        err "SECURITY: Dangerous characters in $type name: $name"
    fi

    return 0
}

print_summary() {
    [[ ${#BACKUP_SUMMARY[@]} -eq 0 ]] && return
    local duration=$(( $(date +%s) - OPERATION_START ))
    log INFO "=== Operation Summary (${duration}s) ==="
    printf '%s\n' "${BACKUP_SUMMARY[@]}" >&2
}

# ---------- Auto-tune parallelism ----------
auto_parallel_jobs() {
  # If user/config already set a positive int, respect it
  if [[ -n "${PARALLEL_JOBS:-}" && "${PARALLEL_JOBS}" =~ ^[0-9]+$ && "${PARALLEL_JOBS}" -ge 1 ]]; then
    echo "$PARALLEL_JOBS"; return 0
  fi

  # Detect physical CPU availability (respecting cgroup quotas if any)
  local host_cpus cfs_quota cfs_period cgroup_cpus
  host_cpus=$(getconf _NPROCESSORS_ONLN 2>/dev/null || nproc 2>/dev/null || echo 1)

  # cgroup v2
  if [[ -r /sys/fs/cgroup/cpu.max ]]; then
    # Format: "<quota> <period>" or "max <period>"
    read -r cfs_quota cfs_period < /sys/fs/cgroup/cpu.max
    if [[ "$cfs_quota" != "max" && "$cfs_quota" =~ ^[0-9]+$ && "$cfs_period" =~ ^[0-9]+$ && "$cfs_period" -gt 0 ]]; then
      cgroup_cpus=$(( (cfs_quota + cfs_period - 1) / cfs_period ))  # ceil
    fi
  fi
  # cgroup v1 fallback
  if [[ -z "${cgroup_cpus:-}" && -r /sys/fs/cgroup/cpu/cpu.cfs_quota_us && -r /sys/fs/cgroup/cpu/cpu.cfs_period_us ]]; then
    cfs_quota=$(cat /sys/fs/cgroup/cpu/cpu.cfs_quota_us 2>/dev/null || echo -1)
    cfs_period=$(cat /sys/fs/cgroup/cpu/cpu.cfs_period_us 2>/dev/null || echo 100000)
    if [[ "$cfs_quota" -gt 0 && "$cfs_period" -gt 0 ]]; then
      cgroup_cpus=$(( (cfs_quota + cfs_period - 1) / cfs_period ))  # ceil
    fi
  fi

  local avail_cpus="${cgroup_cpus:-$host_cpus}"
  (( avail_cpus < 1 )) && avail_cpus=1

  # Disk heuristic: throttle a bit on spinning disks
  local disk_is_rotational=0
  # Try to detect the FS root backing device
  local rootdev
  rootdev=$(df --output=source / 2>/dev/null | awk 'NR==2{print $1}')
  # Map to /sys/block/*/queue/rotational if possible
  if [[ "$rootdev" =~ ^/dev/([a-zA-Z0-9]+) ]]; then
    local blk="${BASH_REMATCH[1]}"
    if [[ -r "/sys/block/$blk/queue/rotational" ]]; then
      if [[ "$(cat /sys/block/$blk/queue/rotational 2>/dev/null)" == "1" ]]; then
        disk_is_rotational=1
      fi
    fi
  fi

  # Current load: if the system is already hot, be conservative
  local load1
  load1=$(awk '{print int($1+0.5)}' /proc/loadavg 2>/dev/null || echo 0)

  # Base: leave headroom for MySQL; target ~60–70% of CPUs
  local base=$(( (avail_cpus * 2) / 3 ))
  (( base < 1 )) && base=1

  # If spinning disk, cap more aggressively (I/O bound)
  local cap
  if (( disk_is_rotational )); then
    cap=$(( avail_cpus / 2 ))
    (( cap < 1 )) && cap=1
  else
    # SSD/NVMe: allow more, but don't go wild
    cap=$(( avail_cpus - 1 ))
    (( cap < 1 )) && cap=1
  fi

  # If load already >= CPUs, halve our plan
  if (( load1 >= avail_cpus )); then
    base=$(( base / 2 ))
    (( base < 1 )) && base=1
  fi

  # Final clamp and reasonable ceiling (avoid pathological values)
  local jobs="$base"
  (( jobs > cap )) && jobs="$cap"
  (( jobs > 12 )) && jobs=12         # hard upper bound to be polite
  (( jobs < 1 ))  && jobs=1

  echo "$jobs"
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
    
    # Set secure umask before creating key file
    local old_umask=$(umask)
    umask 077
    
    log INFO "Generating 256-bit encryption key..."
    
    # Generate a strong random key
    if have openssl; then
        openssl rand -base64 32 > "$key_file"
    elif [[ -r /dev/urandom ]]; then
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
    
    # Restore original umask
    umask "$old_umask"
}

stack_trap 'print_summary' EXIT

# ========================== Signal Handling ==========================

# Global state tracking
declare -g OPERATION_IN_PROGRESS=""
declare -g MYSQL_WAS_STOPPED=0
declare -g DATADIR_BACKUP_PATH=""
declare -g EXPECTED_ERROR_EXIT=0  # Set to 1 when returning error intentionally (e.g., partial backup success)

# Critical cleanup on exit/interrupt
emergency_cleanup() {
    local exit_code=$?
    local signal="${1:-EXIT}"

    # Only log error if this is an actual unexpected error (not normal exit or expected error)
    if [[ $exit_code -ne 0 ]] && [[ "$EXPECTED_ERROR_EXIT" != "1" ]]; then
        log ERROR "Emergency cleanup triggered (signal: $signal, exit code: $exit_code)"
    fi
    
    # If MySQL was stopped during restore, try to restart it
    if [[ "$MYSQL_WAS_STOPPED" == "1" ]]; then
        log ERROR "MySQL was stopped during operation! Attempting restart..."
        
        # If we have a backup of the original datadir, consider restoring it
        if [[ -n "$DATADIR_BACKUP_PATH" ]] && [[ -d "$DATADIR_BACKUP_PATH" ]]; then
            log ERROR "Original datadir backup exists at: $DATADIR_BACKUP_PATH"
            log ERROR "Current /var/lib/mysql may be incomplete!"
            log ERROR "MANUAL INTERVENTION REQUIRED!"
            log ERROR "To rollback: systemctl stop mysql && rm -rf /var/lib/mysql && mv $DATADIR_BACKUP_PATH /var/lib/mysql && systemctl start mysql"
        fi
        
        # Try to start MySQL anyway
        if systemctl start mysql 2>/dev/null || systemctl start mysqld 2>/dev/null || systemctl start mariadb 2>/dev/null; then
            log WARN "MySQL restarted, but database state is UNKNOWN - verify data integrity!"
        else
            log ERROR "Failed to restart MySQL! Database is DOWN!"
            log ERROR "Check logs: journalctl -u mysql -n 100"
        fi
    fi
    
    # Clean up temp directories (only if there was an operation in progress that failed)
    if [[ -n "$OPERATION_IN_PROGRESS" ]] && [[ $exit_code -ne 0 ]]; then
        log WARN "Operation '$OPERATION_IN_PROGRESS' was interrupted"
        
        # Clean up any .partial files
        find "$BACKUP_DIR" -name "*.partial" -type f -delete 2>/dev/null || true
        
        # Clean up temp restore directories
        find "$BACKUP_DIR" -name ".xtra_restore_*" -type d -exec rm -rf {} + 2>/dev/null || true
        find "$BACKUP_DIR" -name ".xtra_base_*" -type d -exec rm -rf {} + 2>/dev/null || true
    fi
    
    # Release lock
    release_lock
    
    # Send notification if this was an error
    if [[ $exit_code -ne 0 ]] && [[ -n "$OPERATION_IN_PROGRESS" ]]; then
        notify "db-tools FAILED: $OPERATION_IN_PROGRESS" \
               "Operation interrupted with exit code $exit_code. Check logs immediately!" \
               "error"
    fi
}

# Set up signal handlers
trap 'emergency_cleanup SIGINT' SIGINT   # Ctrl+C
trap 'emergency_cleanup SIGTERM' SIGTERM # kill
trap 'emergency_cleanup SIGHUP' SIGHUP   # Terminal closed
trap 'emergency_cleanup EXIT' EXIT       # Normal/abnormal exit

# ========================== Lock Management ==========================

acquire_lock() {
  local timeout="${1:-300}"
  local operation="${2:-operation}"  # What operation is locking

  if have flock; then
    # Use FD 9 for the lock
    exec 9>"$LOCK_FILE"
    if ! flock -n 9; then
      # Read what process holds the lock
      local lock_info
      if [[ -f "$LOCK_FILE" ]]; then
        lock_info=$(cat "$LOCK_FILE" 2>/dev/null || echo "unknown")
        err "Another db-tools instance is already running: $lock_info (lock: $LOCK_FILE)"
      else
        err "Another db-tools instance holds the lock ($LOCK_FILE)."
      fi
    fi
    
    # Write lock info
    echo "PID:$$ OPERATION:$operation USER:$(whoami) STARTED:$(date '+%Y-%m-%d %H:%M:%S')" > "$LOCK_FILE"
    stack_trap 'release_lock' EXIT
    debug "Lock acquired for '$operation' (PID: $$)"
    return
  fi

  # Fallback to manual lock file with timeout and PID checking
  # Use atomic file creation with noclobber to prevent race conditions
  local elapsed=0
  local lock_content="PID:$$ OPERATION:$operation USER:$(whoami) STARTED:$(date '+%Y-%m-%d %H:%M:%S')"

  while true; do
    # Try atomic lock creation (noclobber prevents overwriting existing file)
    if (set -o noclobber; echo "$lock_content" > "$LOCK_FILE") 2>/dev/null; then
      # Successfully created lock file
      stack_trap 'release_lock' EXIT
      debug "Lock acquired for '$operation' (PID: $$)"
      return 0
    fi

    # Lock file exists, check if it's stale
    if [[ -f "$LOCK_FILE" ]]; then
      local lock_info=$(cat "$LOCK_FILE" 2>/dev/null || echo "unknown")

      if [[ "$lock_info" =~ PID:([0-9]+) ]]; then
        local lock_pid="${BASH_REMATCH[1]}"
        if ! kill -0 "$lock_pid" 2>/dev/null; then
          warn "Stale lock file found (PID $lock_pid not running), removing..."
          rm -f "$LOCK_FILE"
          continue  # Retry lock acquisition
        fi
      fi

      if (( elapsed >= timeout )); then
        err "Lock timeout: Another instance running for ${elapsed}s. Lock info: $lock_info"
      fi

      debug "Waiting for lock... (${elapsed}s) - Held by: $lock_info"
    fi

    sleep 5
    ((elapsed+=5))
  done
}

release_lock() {
  # Close FD if flock path used
  { exec 9>&-; } 2>/dev/null || true
  rm -f "$LOCK_FILE"
  debug "Lock released"
}

# Helper to check if operation is locked
is_locked() {
  if [[ -f "$LOCK_FILE" ]]; then
    local lock_info=$(cat "$LOCK_FILE" 2>/dev/null)
    echo "yes: $lock_info"
    return 0
  else
    echo "no"
    return 1
  fi
}

# ========================== Configuration Loading ==========================

load_config() {
    # Safe config loader - parses key=value instead of sourcing
    _safe_load_config() {
        local file="$1"
        [[ -f "$file" ]] || return 0

        # Only allow files owned by root or current user with safe permissions
        local file_owner file_perms
        if file_owner=$(stat -c '%U' "$file" 2>/dev/null); then
            file_perms=$(stat -c '%a' "$file" 2>/dev/null)
        else
            file_owner=$(stat -f '%Su' "$file" 2>/dev/null || echo "")
            file_perms=$(stat -f '%Lp' "$file" 2>/dev/null || echo "777")
        fi

        if [[ -n "$file_owner" && "$file_owner" != "root" && "$file_owner" != "$USER" ]]; then
            warn "Skipping config file owned by '$file_owner': $file"
            return 1
        fi

        # Parse key=value pairs safely (only uppercase alphanumeric + underscore keys)
        while IFS= read -r line || [[ -n "$line" ]]; do
            # Skip comments and empty lines
            [[ "$line" =~ ^[[:space:]]*# ]] && continue
            [[ "$line" =~ ^[[:space:]]*$ ]] && continue

            # Extract key=value, allowing quoted values
            if [[ "$line" =~ ^[[:space:]]*([A-Z_][A-Z0-9_]*)[[:space:]]*=[[:space:]]*(.*)[[:space:]]*$ ]]; then
                local key="${BASH_REMATCH[1]}"
                local value="${BASH_REMATCH[2]}"
                # Strip surrounding quotes if present
                value="${value#\"}" ; value="${value%\"}"
                value="${value#\'}" ; value="${value%\'}"
                # Export the variable
                export "$key=$value"
            fi
        done < "$file"
        debug "Configuration loaded from $file"
    }

    # Load from .env if present (local override)
    _safe_load_config "$ENV_FILE"

    # Load from system config
    _safe_load_config "$CONFIG_FILE"
}

# ========================== Tool Checking ==========================

check_core_dependencies() {
    local missing=()
    for tool in curl awk sed grep cut date tr head tail sort uniq; do
        if ! have "$tool"; then
            missing+=("$tool")
        fi
    done
    
    if [[ ${#missing[@]} -gt 0 ]]; then
        err "Missing core dependencies: ${missing[*]}. Please install them."
    fi
}

need_tooling() {
    [[ -n "$MYSQL" ]]     || err "mysql client not found"
    [[ -n "$MYSQLDUMP" ]] || warn "mysqldump not found (logical backups will be unavailable)"
    [[ -n "$MYSQLBINLOG" ]] || warn "mysqlbinlog not found (PITR will be limited)"
    
    # Auto-install XtraBackup if backup method is xtrabackup and it's not found
    if [[ "$BACKUP_METHOD" == "xtrabackup" ]]; then
        if [[ -z "$XTRABACKUP" ]]; then
            # Check if we already tried and failed
            if [[ -f "$MARK_DIR/xtrabackup-install-failed" ]]; then
                debug "XtraBackup install previously failed, using mysqldump"
                BACKUP_METHOD="mysqldump"
                return
            fi
            
            # Check if auto-install is enabled
            if [[ "$AUTO_INSTALL" != "1" ]]; then
                warn "XtraBackup not found. Set AUTO_INSTALL=1 to enable auto-install, or install manually."
                warn "Falling back to mysqldump for this operation."
                BACKUP_METHOD="mysqldump"
                return
            fi
            
            log INFO "XtraBackup not found, attempting auto-install..."
            if install_xtrabackup; then
                log INFO "✅ XtraBackup auto-installed successfully"
                # Update the path after installation
                XTRABACKUP="$(command -v xtrabackup || command -v mariabackup || true)"
            else
                warn "XtraBackup auto-install failed, falling back to mysqldump"
                BACKUP_METHOD="mysqldump"
                # Mark that we tried and failed (avoid repeated attempts)
                mkdir -p "$MARK_DIR"
                touch "$MARK_DIR/xtrabackup-install-failed"
            fi
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
                if ! install_package pigz; then
                    warn "Failed to install pigz, falling back to gzip"
                    COMPRESS_ALGO="gzip"
                fi
            fi
            ;;
        zstd)
            if ! have zstd; then
                warn "zstd not found, attempting install..."
                if ! install_package zstd; then
                    warn "Failed to install zstd, falling back to gzip"
                    COMPRESS_ALGO="gzip"
                fi
            fi
            ;;
        xz)
            if ! have xz; then
                warn "xz-utils not found, attempting install..."
                if ! install_package xz-utils && ! install_package xz; then
                    warn "Failed to install xz, falling back to gzip"
                    COMPRESS_ALGO="gzip"
                fi
            fi
            ;;
    esac
}
check_key_perms() {
  [[ "$ENCRYPT_BACKUPS" == "1" && -n "$ENCRYPTION_KEY_FILE" ]] || return 0

  # Must exist and not be a symlink
  [[ -e "$ENCRYPTION_KEY_FILE" ]] || err "Encryption key file not found: $ENCRYPTION_KEY_FILE"
  if [[ -L "$ENCRYPTION_KEY_FILE" ]]; then
    err "SECURITY: Key file is a symlink. Use a regular file at $ENCRYPTION_KEY_FILE"
  fi

  # Ownership: prefer root or current user
  local owner=""
  if owner=$(stat -c '%U' "$ENCRYPTION_KEY_FILE" 2>/dev/null); then
    :
  else
    owner=$(stat -f '%Su' "$ENCRYPTION_KEY_FILE" 2>/dev/null || echo "")
  fi
  if [[ -n "$owner" && "$owner" != "root" && "$owner" != "$USER" ]]; then
    warn "Encryption key owned by '$owner' (expected root or $USER)"
  fi

  # Permissions: 400 or 600 acceptable; nothing broader.
  local perms=""
  if perms=$(stat -c '%a' "$ENCRYPTION_KEY_FILE" 2>/dev/null); then
    :
  else
    perms=$(stat -f '%Lp' "$ENCRYPTION_KEY_FILE" 2>/dev/null || echo "000")
  fi
  # strip leading zeros if any
  perms="${perms##+(0)}"
  [[ -z "$perms" ]] && perms="0"

  # numeric compare (treat anything > 600 as too broad)
  if (( 10#$perms > 600 )); then
    err "SECURITY: Key permissions are $perms (must be 600 or stricter). Fix with: chmod 600 $ENCRYPTION_KEY_FILE"
  fi

  # ACL check (if tools present)
  if have getfacl; then
    # Allow only owner entry and mask/defaults; no named users/groups with extra perms
    local acl_extra
    acl_extra=$(getfacl -p "$ENCRYPTION_KEY_FILE" 2>/dev/null | grep -E '^(user:|group:)[^:]+:' | grep -vE "^user::|^group::" || true)
    if [[ -n "$acl_extra" ]]; then
      warn "SECURITY: Key has extra ACL entries. Consider: setfacl -b $ENCRYPTION_KEY_FILE"
    fi
  fi

  # Content sanity (should look like a 256-bit base64 blob from generate_encryption_key)
  # 32 bytes → base64 typically 44 chars with padding, but tolerate variations
  local sample
  sample="$(head -c 64 "$ENCRYPTION_KEY_FILE" 2>/dev/null | tr -d '\n' || true)"
  if [[ -z "$sample" ]]; then
    err "Key file is empty: $ENCRYPTION_KEY_FILE"
  fi
  if ! echo "$sample" | tr -d '[:space:]' | grep -Eq '^[A-Za-z0-9+/]+=*$'; then
    warn "Key file does not appear to be base64; ensure it was created by 'db-tools genkey'"
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

    log INFO "Installing $pkg..."
    if have apt-get; then
        export DEBIAN_FRONTEND=noninteractive
        apt-get update -y >/dev/null 2>&1 || true
        if apt-get install -y "$pkg" >/dev/null 2>&1; then
            log INFO "✅ $pkg installed"
            return 0
        else
            warn "apt-get install $pkg failed"
            return 1
        fi
    elif have yum; then
        if yum -y install "$pkg" >/dev/null 2>&1; then
            log INFO "✅ $pkg installed"
            return 0
        else
            warn "yum install $pkg failed"
            return 1
        fi
    elif have dnf; then
        if dnf -y install "$pkg" >/dev/null 2>&1; then
            log INFO "✅ $pkg installed"
            return 0
        else
            warn "dnf install $pkg failed"
            return 1
        fi
    else
        warn "No supported package manager found"
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

get_binlog_extension() {
    case "$COMPRESS_ALGO" in
        zstd) echo "binlog.zst" ;;
        xz)   echo "binlog.xz"  ;;
        pigz|gzip|*) echo "binlog.gz" ;;
    esac
}

# ========================== Encryption Functions ==========================

encrypt_if_enabled() {
    if [[ "$ENCRYPT_BACKUPS" == "1" ]] && [[ -n "$ENCRYPTION_KEY_FILE" ]] && [[ -f "$ENCRYPTION_KEY_FILE" ]]; then
        # Use AES-256-GCM for authenticated encryption (AEAD)
        openssl enc -aes-256-gcm -salt -pbkdf2 -pass file:"$ENCRYPTION_KEY_FILE"
    else
        cat
    fi
}

decrypt_if_encrypted() {
    local file="$1"
    if [[ "$file" =~ \.enc$ ]] && [[ -n "$ENCRYPTION_KEY_FILE" ]] && [[ -f "$ENCRYPTION_KEY_FILE" ]]; then
        # Use AES-256-GCM for authenticated decryption
        openssl enc -d -aes-256-gcm -pbkdf2 -pass file:"$ENCRYPTION_KEY_FILE"
    else
        cat
    fi
}

# Strip/neutralize fragile clauses from dumps so restores succeed across hosts
sanitize_dump_stream() {
    # - Neutralize DEFINERs (procedures, views, triggers, events)
    # - Remove versioned comments that set DEFINER (/*!50013 ... */ etc.)
    # - Disable binary logging during import to avoid replica storms
    # - Relax foreign key checks for faster & safer bulk load
    # - Ensure SQL mode doesn't break old dumps
    awk 'BEGIN {
            print "SET @OLD_SQL_LOG_BIN=@@SQL_LOG_BIN; SET SQL_LOG_BIN=0;";
            print "SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS; SET FOREIGN_KEY_CHECKS=0;";
            print "SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS; SET UNIQUE_CHECKS=0;";
            print "SET @OLD_SQL_MODE=@@SQL_MODE; SET SQL_MODE=\"\";";
        }
        { print }
        END {
            print "SET SQL_MODE=@OLD_SQL_MODE;";
            print "SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;";
            print "SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;";
            print "SET SQL_LOG_BIN=@OLD_SQL_LOG_BIN;";
        }' \
    | sed -E \
        -e 's/\/\*!([0-9]{5})[[:space:]]+DEFINER=`[^`]+`@`[^`]+`[[:space:]]*/\/*!\1 /g' \
        -e 's/DEFINER=`[^`]+`@`[^`]+`/DEFINER=CURRENT_USER/g'
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
        # Use jq for proper JSON escaping if available, otherwise escape manually
        local payload
        if have jq; then
            payload=$(jq -n --arg s "$subject" --arg m "$message" --arg l "$level" --arg h "$(hostname)" \
                '{subject: $s, message: $m, level: $l, host: $h}')
        else
            # Manual escaping: replace \ with \\, " with \", and newlines
            local esc_subject esc_message
            esc_subject=$(printf '%s' "$subject" | sed 's/\\/\\\\/g; s/"/\\"/g' | tr '\n' ' ')
            esc_message=$(printf '%s' "$message" | sed 's/\\/\\\\/g; s/"/\\"/g' | tr '\n' ' ')
            payload="{\"subject\":\"$esc_subject\",\"message\":\"$esc_message\",\"level\":\"$level\",\"host\":\"$(hostname)\"}"
        fi
        curl -s -X POST "$NOTIFY_WEBHOOK" \
            -H "Content-Type: application/json" \
            -d "$payload" \
            >/dev/null 2>&1 || true
    fi
}

# ========================== MySQL Functions ==========================
# ========================== Service Control Functions ==========================

start_mysql() {
    log INFO "Starting MySQL/MariaDB..."
    
    # Try systemd first
    if systemctl start mysql 2>/dev/null; then
        return 0
    elif systemctl start mysqld 2>/dev/null; then
        return 0
    elif systemctl start mariadb 2>/dev/null; then
        return 0
    # Fallback to sysvinit
    elif command -v service >/dev/null 2>&1; then
        if service mysql start 2>/dev/null; then
            return 0
        elif service mysqld start 2>/dev/null; then
            return 0
        fi
    fi
    
    # Last resort: try init.d directly
    if [[ -x /etc/init.d/mysql ]]; then
        /etc/init.d/mysql start
        return $?
    elif [[ -x /etc/init.d/mysqld ]]; then
        /etc/init.d/mysqld start
        return $?
    fi
    
    return 1
}

stop_mysql() {
    log INFO "Stopping MySQL/MariaDB..."
    
    # Try systemd first
    if systemctl stop mysql 2>/dev/null; then
        return 0
    elif systemctl stop mysqld 2>/dev/null; then
        return 0
    elif systemctl stop mariadb 2>/dev/null; then
        return 0
    # Fallback to sysvinit
    elif command -v service >/dev/null 2>&1; then
        if service mysql stop 2>/dev/null; then
            return 0
        elif service mysqld stop 2>/dev/null; then
            return 0
        fi
    fi
    
    # Last resort: try init.d directly
    if [[ -x /etc/init.d/mysql ]]; then
        /etc/init.d/mysql stop
        return $?
    elif [[ -x /etc/init.d/mysqld ]]; then
        /etc/init.d/mysqld stop
        return $?
    fi
    
    return 1
}

is_mysql_running() {
    if systemctl is-active --quiet mysql 2>/dev/null; then
        return 0
    elif systemctl is-active --quiet mysqld 2>/dev/null; then
        return 0
    elif systemctl is-active --quiet mariadb 2>/dev/null; then
        return 0
    elif command -v service >/dev/null 2>&1; then
        if service mysql status >/dev/null 2>&1; then
            return 0
        elif service mysqld status >/dev/null 2>&1; then
            return 0
        fi
    fi
    
    # Check if MySQL process is running
    if pgrep -x mysqld >/dev/null 2>&1 || pgrep -x mariadbd >/dev/null 2>&1; then
        return 0
    fi
    
    return 1
}

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

check_restore_space() {
    local backup_file="$1"
    local multiplier="${2:-3}"  # Need 3x: extract + prepare + copy
    
    # Get compressed backup size
    local backup_size_mb=$(du -m "$backup_file" | cut -f1)
    
    # Estimate uncompressed size (assume 5:1 compression ratio)
    local uncompressed_mb=$((backup_size_mb * 5))
    
    # Need space for: extraction + working directory + MySQL datadir
    local required_mb=$((uncompressed_mb * multiplier))
    
    # Check backup directory space
    local backup_avail=$(df -BM "$BACKUP_DIR" | awk 'NR==2 {print $4}' | sed 's/M//')
    local datadir_avail=$(df -BM /var/lib/mysql | awk 'NR==2 {print $4}' | sed 's/M//')
    
    log INFO "Disk space check:"
    log INFO "  Backup file: ${backup_size_mb}MB compressed"
    log INFO "  Estimated uncompressed: ${uncompressed_mb}MB"
    log INFO "  Required (${multiplier}x): ${required_mb}MB"
    log INFO "  Available in backup dir: ${backup_avail}MB"
    log INFO "  Available in MySQL datadir: ${datadir_avail}MB"
    
    if (( backup_avail < required_mb )); then
        err "Insufficient disk space in $BACKUP_DIR! Need ${required_mb}MB, have ${backup_avail}MB"
    fi
    
    if (( datadir_avail < uncompressed_mb )); then
        err "Insufficient disk space in /var/lib/mysql! Need ${uncompressed_mb}MB, have ${datadir_avail}MB"
    fi
    
    log INFO "✅ Sufficient disk space available"
}

check_backup_space() {
    local backup_dir="$1"
    local estimated_size_mb="$2"
    local multiplier="${3:-4}"  # Need 4x: raw backup + compressed + working space + buffer
    
    local required_mb=$((estimated_size_mb * multiplier))
    local available=$(df -BM "$backup_dir" | awk 'NR==2 {print $4}' | sed 's/M//')
    
    log INFO "Backup space check:"
    log INFO "  Estimated DB size: ${estimated_size_mb}MB"
    log INFO "  Required (${multiplier}x): ${required_mb}MB"
    log INFO "  Available: ${available}MB"
    
    if (( available < required_mb )); then
        err "Insufficient disk space! Need ${required_mb}MB, have ${available}MB. Free up space or increase retention."
    fi
    
    if (( available < required_mb * 2 )); then
        warn "Low disk space! Have ${available}MB, recommended ${required_mb * 2}MB"
    fi
    
    log INFO "✅ Sufficient disk space available"
}

# ========================== Smart Space Management ==========================

# Get total size of all backups in MB
get_backup_total_size_mb() {
    local total=0
    shopt -s nullglob
    for f in "$BACKUP_DIR"/*.sql.* "$BACKUP_DIR"/*.tar "$BACKUP_DIR"/*.tar.* "$BACKUP_DIR"/*.binlog.*; do
        [[ -f "$f" ]] || continue
        [[ "$f" =~ \.(sha256|meta)$ ]] && continue
        local size_mb
        size_mb=$(du -m "$f" 2>/dev/null | cut -f1 || echo 0)
        total=$((total + size_mb))
    done
    echo "$total"
}

# Get disk usage percentage for backup directory
get_disk_usage_percent() {
    local pct
    pct=$(df "$BACKUP_DIR" 2>/dev/null | awk 'NR==2 {gsub(/%/,"",$5); print $5}')
    # Return 0 if empty or invalid
    [[ "$pct" =~ ^[0-9]+$ ]] && echo "$pct" || echo "0"
}

# Get free space in GB
get_free_space_gb() {
    local free_kb
    free_kb=$(df "$BACKUP_DIR" 2>/dev/null | awk 'NR==2 {print $4}')
    # Handle empty or invalid values
    [[ "$free_kb" =~ ^[0-9]+$ ]] || free_kb=0
    echo $((free_kb / 1024 / 1024))
}

# Clean up orphaned .partial files (failed backups)
cleanup_partial_files() {
    [[ "$SPACE_CLEANUP_PARTIAL" != "1" ]] && return 0
    [[ -d "$BACKUP_DIR" ]] || return 0

    local count=0
    local now
    now=$(date +%s)

    while IFS= read -r f; do
        [[ -f "$f" ]] || continue
        # Only remove .partial files older than 1 hour (3600 seconds)
        local mtime
        if mtime=$(stat -c %Y "$f" 2>/dev/null); then
            :
        else
            mtime=$(stat -f %m "$f" 2>/dev/null || echo "$now")
        fi
        local age=$((now - mtime))
        if (( age > 3600 )); then
            log INFO "Removing orphaned partial file: $(basename "$f")"
            rm -f "$f"
            ((count++)) || true
        fi
    done < <(find "$BACKUP_DIR" -maxdepth 1 -name "*.partial" -type f 2>/dev/null)

    # Also clean up temp directories from interrupted XtraBackup operations
    while IFS= read -r d; do
        [[ -d "$d" ]] || continue
        local mtime
        if mtime=$(stat -c %Y "$d" 2>/dev/null); then
            :
        else
            mtime=$(stat -f %m "$d" 2>/dev/null || echo "$now")
        fi
        local age=$((now - mtime))
        if (( age > 3600 )); then
            log INFO "Removing orphaned temp directory: $(basename "$d")"
            rm -rf "$d"
            ((count++)) || true
        fi
    done < <(find "$BACKUP_DIR" -maxdepth 1 -type d \( -name ".xtra_*" -o -name ".job_status.*" \) 2>/dev/null)

    if (( count > 0 )); then
        log INFO "Cleaned up $count orphaned file(s)/directory(s)"
    fi
}

# Check space and send warning notifications
check_space_warnings() {
    local usage_percent free_gb
    usage_percent=$(get_disk_usage_percent)
    free_gb=$(get_free_space_gb)

    # Use defaults if config vars are not set
    local critical_pct=${SPACE_CRITICAL_PERCENT:-90}
    local warning_pct=${SPACE_WARNING_PERCENT:-70}
    local min_free=${SPACE_MIN_FREE_GB:-5}

    if (( usage_percent >= critical_pct )); then
        warn "CRITICAL: Disk usage at ${usage_percent}% (threshold: ${critical_pct}%)"
        notify "CRITICAL: Backup disk space" \
               "Disk usage at ${usage_percent}% on $(hostname). Free: ${free_gb}GB. Immediate action required!" \
               "error"
        return 2
    elif (( usage_percent >= warning_pct )); then
        warn "WARNING: Disk usage at ${usage_percent}% (threshold: ${warning_pct}%)"
        notify "WARNING: Backup disk space low" \
               "Disk usage at ${usage_percent}% on $(hostname). Free: ${free_gb}GB. Consider cleanup." \
               "error"
        return 1
    fi

    if (( free_gb < min_free )); then
        warn "WARNING: Only ${free_gb}GB free (minimum: ${min_free}GB)"
        notify "WARNING: Low free disk space" \
               "Only ${free_gb}GB free on $(hostname). Minimum required: ${min_free}GB." \
               "error"
        return 1
    fi

    debug "Disk space OK: ${usage_percent}% used, ${free_gb}GB free"
    return 0
}

# Get list of backups sorted by age (oldest first)
get_backups_by_age() {
    shopt -s nullglob
    local files=()
    for f in "$BACKUP_DIR"/*.sql.* "$BACKUP_DIR"/*.tar "$BACKUP_DIR"/*.tar.* "$BACKUP_DIR"/*.binlog.*; do
        [[ -f "$f" ]] || continue
        [[ "$f" =~ \.(sha256|meta)$ ]] && continue
        files+=("$f")
    done

    # Sort by modification time (oldest first)
    for f in "${files[@]}"; do
        local mtime
        if mtime=$(stat -c %Y "$f" 2>/dev/null); then
            :
        else
            mtime=$(stat -f %m "$f" 2>/dev/null || echo 0)
        fi
        echo "$mtime $f"
    done | sort -n | cut -d' ' -f2-
}

# Count backups per database
count_backups_per_db() {
    local db="$1"
    local count=0
    shopt -s nullglob
    for f in "$BACKUP_DIR"/"$db"-*.sql.*; do
        [[ -f "$f" ]] || continue
        [[ "$f" =~ \.(sha256|meta)$ ]] && continue
        ((count++)) || true
    done
    echo "$count"
}

# Smart cleanup: remove oldest backups to free space
# Respects CLEAN_KEEP_MIN per database
smart_cleanup_for_space() {
    local needed_mb="$1"
    local freed_mb=0
    local removed_count=0

    log INFO "Smart cleanup: need to free ${needed_mb}MB"

    # Build list of deletable files (respecting minimum keep per DB)
    declare -A db_counts=()
    local deletable_files=()

    # First pass: count backups per database
    while IFS= read -r f; do
        [[ -z "$f" ]] && continue
        local base
        base=$(basename "$f")
        local db="${base%%-[0-9][0-9][0-9][0-9]-*}"
        if [[ -n "$db" ]]; then
            db_counts["$db"]=$(( ${db_counts["$db"]:-0} + 1 ))
        fi
    done < <(get_backups_by_age)

    # Second pass: mark files as deletable if we have more than CLEAN_KEEP_MIN
    while IFS= read -r f; do
        [[ -z "$f" ]] && continue
        local base
        base=$(basename "$f")
        local db="${base%%-[0-9][0-9][0-9][0-9]-*}"

        # For XtraBackup files, extract differently
        if [[ "$base" =~ ^xtra-(full|incr)- ]]; then
            db="__xtrabackup__"
        fi

        if [[ -n "$db" && ${db_counts["$db"]:-0} -gt $CLEAN_KEEP_MIN ]]; then
            deletable_files+=("$f")
            db_counts["$db"]=$(( ${db_counts["$db"]} - 1 ))
        fi
    done < <(get_backups_by_age)

    # Delete oldest files until we have enough space
    for f in "${deletable_files[@]}"; do
        (( freed_mb >= needed_mb )) && break

        local size_mb
        size_mb=$(du -m "$f" 2>/dev/null | cut -f1 || echo 0)

        if (( DRY_RUN )); then
            log INFO "DRY-RUN: Would delete $(basename "$f") (${size_mb}MB)"
        else
            log INFO "Deleting for space: $(basename "$f") (${size_mb}MB)"
            rm -f "$f" "${f}.sha256" 2>/dev/null || true

            # Also remove associated metadata if it exists
            local ts
            ts=$(echo "$f" | grep -oE '[0-9]{4}-[0-9]{2}-[0-9]{2}-[0-9]{2}-[0-9]{2}-[0-9]{2}' || true)
            if [[ -n "$ts" ]]; then
                rm -f "$BACKUP_DIR/backup-$ts.meta" 2>/dev/null || true
            fi
        fi

        freed_mb=$((freed_mb + size_mb))
        ((removed_count++))
    done

    log INFO "Smart cleanup complete: freed ${freed_mb}MB by removing $removed_count file(s)"
    echo "$freed_mb"
}

# Enforce size-based retention (total backup size limit)
enforce_size_limit() {
    # Skip if not configured or set to 0/empty
    [[ -z "${SPACE_MAX_USAGE_GB:-}" ]] && return 0
    [[ "${SPACE_MAX_USAGE_GB:-0}" -le 0 ]] 2>/dev/null && return 0

    local max_mb=$((SPACE_MAX_USAGE_GB * 1024))
    local current_mb
    current_mb=$(get_backup_total_size_mb)

    if (( current_mb <= max_mb )); then
        debug "Backup size OK: ${current_mb}MB / ${max_mb}MB"
        return 0
    fi

    local excess_mb=$((current_mb - max_mb))
    log INFO "Backup size ${current_mb}MB exceeds limit ${max_mb}MB, need to free ${excess_mb}MB"

    smart_cleanup_for_space "$excess_mb"
}

# Pre-backup space check with auto-cleanup
ensure_space_for_backup() {
    local estimated_size_mb="${1:-0}"
    local multiplier="${2:-4}"

    # Validate inputs
    [[ "$estimated_size_mb" =~ ^[0-9]+$ ]] || estimated_size_mb=0
    [[ "$multiplier" =~ ^[0-9]+$ ]] || multiplier=4

    local required_mb=$((estimated_size_mb * multiplier))
    debug "ensure_space_for_backup: estimated=${estimated_size_mb}MB, multiplier=${multiplier}, required=${required_mb}MB"

    # Clean partial files first
    debug "Cleaning partial files..."
    cleanup_partial_files || true

    # Check warnings
    debug "Checking space warnings..."
    check_space_warnings || true  # Don't fail, just warn

    # Enforce size limits
    debug "Enforcing size limits..."
    enforce_size_limit || true

    # Check if we have enough space
    local available_mb
    available_mb=$(df -BM "$BACKUP_DIR" 2>/dev/null | awk 'NR==2 {gsub(/M/,"",$4); print $4}')
    # Handle empty or invalid values
    [[ "$available_mb" =~ ^[0-9]+$ ]] || available_mb=0

    if (( available_mb >= required_mb )); then
        debug "Sufficient space available: ${available_mb}MB >= ${required_mb}MB"
        return 0
    fi

    # Need more space
    local needed_mb=$((required_mb - available_mb + 1024))  # Add 1GB buffer
    log INFO "Need ${needed_mb}MB more space for backup"

    if [[ "$SPACE_AUTO_CLEANUP" != "1" ]]; then
        err "Insufficient space (${available_mb}MB available, ${required_mb}MB needed). Enable SPACE_AUTO_CLEANUP=1 or free space manually."
    fi

    # Try to free space
    local freed_mb
    freed_mb=$(smart_cleanup_for_space "$needed_mb")

    # Re-check
    available_mb=$(df -BM "$BACKUP_DIR" 2>/dev/null | awk 'NR==2 {gsub(/M/,"",$4); print $4}')

    if (( available_mb < required_mb )); then
        # Check if we're at minimum keep level
        warn "Could only free ${freed_mb}MB (keeping minimum $CLEAN_KEEP_MIN backups per database)"

        # Calculate what we have vs need
        local shortfall=$((required_mb - available_mb))

        if (( available_mb >= estimated_size_mb * 2 )); then
            # We have at least 2x the estimated size, proceed with warning
            warn "Proceeding with reduced buffer: ${available_mb}MB available"
        else
            err "Cannot free enough space. Have ${available_mb}MB, need ${required_mb}MB. Reduce CLEAN_KEEP_MIN or free space manually."
        fi
    fi

    log INFO "✅ Space ready for backup: ${available_mb}MB available"
}

# Show space usage summary
show_space_summary() {
    local total_mb total_gb usage_percent free_gb
    total_mb=$(get_backup_total_size_mb)
    [[ "$total_mb" =~ ^[0-9]+$ ]] || total_mb=0
    total_gb=$((total_mb / 1024))
    usage_percent=$(get_disk_usage_percent)
    free_gb=$(get_free_space_gb)

    echo "=== Backup Space Summary ==="
    echo "  Total backup size:    ${total_gb}GB (${total_mb}MB)"
    echo "  Disk usage:           ${usage_percent}%"
    echo "  Free space:           ${free_gb}GB"

    if [[ "${SPACE_MAX_USAGE_GB:-0}" -gt 0 ]] 2>/dev/null; then
        echo "  Size limit:           ${SPACE_MAX_USAGE_GB}GB"
    fi
    echo "  Warning threshold:    ${SPACE_WARNING_PERCENT:-70}%"
    echo "  Critical threshold:   ${SPACE_CRITICAL_PERCENT:-90}%"
    echo "  Min free space:       ${SPACE_MIN_FREE_GB:-5}GB"
    echo

    # Count backups
    local backup_count=0
    shopt -s nullglob
    for f in "$BACKUP_DIR"/*.sql.* "$BACKUP_DIR"/*.tar "$BACKUP_DIR"/*.tar.*; do
        [[ -f "$f" ]] || continue
        [[ "$f" =~ \.(sha256|meta)$ ]] && continue
        ((backup_count++))
    done
    echo "  Total backup files:   $backup_count"
    echo "  Minimum keep per DB:  $CLEAN_KEEP_MIN"
}

# ========================== Progress Functions ==========================

# ========================== Initialization ==========================
init() {
    need_tooling
    log INFO "Initializing db-tools with login-path '$LOGIN_PATH'..."

    have mysql_config_editor || err "mysql_config_editor not found"

    # -------- Gather MySQL credentials --------
    if [[ -t 0 ]]; then
        read -r -p "MySQL host [localhost]: " host; host=${host:-localhost}
        # trim whitespace
        host="${host#"${host%%[![:space:]]*}"}"; host="${host%"${host##*[![:space:]]}"}"

        read -r -p "MySQL port [3306]: " port; port=${port:-3306}
        port="${port#"${port%%[![:space:]]*}"}"; port="${port%"${port##*[![:space:]]}"}"
        [[ "$port" =~ ^[0-9]+$ ]] || err "Invalid port: $port"

        read -r -p "MySQL admin user [root]: " user; user=${user:-root}
        user="${user#"${user%%[![:space:]]*}"}"; user="${user%"${user##*[![:space:]]}"}"

        read -r -s -p "MySQL password for '$user' (leave blank if none): " pass; echo
    else
        # Non-tty: allow env overrides, don't block on reads
        host="${host:-localhost}"
        port="${port:-3306}"
        [[ "$port" =~ ^[0-9]+$ ]] || err "Invalid port: $port"
        user="${user:-root}"
        pass="${DBTOOLS_PASSWORD:-${MYSQL_PWD:-}}"
    fi

    [[ -n "$host" ]] || err "Host cannot be empty"

    if [[ -z "$pass" ]]; then
        warn "Empty password being set for login-path '$LOGIN_PATH'"
    fi

    # -------- Configure mysql_config_editor (non-interactive) --------
    mysql_config_editor remove --login-path="$LOGIN_PATH" >/dev/null 2>&1 || true

    printf '%s\n' "$pass" | mysql_config_editor set \
        --login-path="$LOGIN_PATH" \
        --host="$host" \
        --user="$user" \
        --port="$port" \
        --password

    # Quick connectivity test
    "$MYSQL" --login-path="$LOGIN_PATH" -e "SELECT VERSION();" >/dev/null \
        || err "Login test failed"

    # Also write a secure defaults file for tools like xtrabackup
    write_mysql_defaults_file "$host" "$port" "$user" "$pass"

    # -------- Gather backup settings (after MySQL is configured) --------
    local backup_dir_input=""
    if [[ -t 0 ]]; then
        echo
        read -r -p "Backup directory [$BACKUP_DIR]: " backup_dir_input
        backup_dir_input="${backup_dir_input:-$BACKUP_DIR}"
        # trim whitespace
        backup_dir_input="${backup_dir_input#"${backup_dir_input%%[![:space:]]*}"}"
        backup_dir_input="${backup_dir_input%"${backup_dir_input##*[![:space:]]}"}"
    else
        backup_dir_input="${BACKUP_DIR}"
    fi

    # Update BACKUP_DIR if a new value was provided
    if [[ -n "$backup_dir_input" ]]; then
        BACKUP_DIR="$backup_dir_input"
    fi

    # -------- Optional helper tooling --------
    log INFO "Installing additional tools..."
    if have apt-get; then
        export DEBIAN_FRONTEND=noninteractive
        apt-get update -y >/dev/null 2>&1 || true
        apt-get install -y percona-toolkit mysqltuner mailutils qpress >/dev/null 2>&1 \
            || warn "Some tools failed to install"
    elif have yum; then
        yum -y install percona-toolkit mysqltuner mailx qpress >/dev/null 2>&1 \
            || warn "Some tools failed to install"
    elif have dnf; then
        dnf -y install percona-toolkit mysqltuner mailx qpress >/dev/null 2>&1 \
            || warn "Some tools failed to install"
    fi

    ensure_compression_tools
    check_key_perms

    mkdir -p "$BACKUP_DIR" "$MARK_DIR" "$LOG_DIR"
    date -Is > "$MARK_INIT"

    # Save essential settings to config file if it doesn't exist or backup dir changed
    if [[ ! -f "$CONFIG_FILE" ]] || [[ "$BACKUP_DIR" != "/var/backups/mysql" ]]; then
        log INFO "Saving configuration to $CONFIG_FILE..."
        local old_umask
        old_umask=$(umask)
        umask 077
        cat > "$CONFIG_FILE" <<INITCONF
# db-tools configuration (created by init)
# Edit this file to customize settings, or run: db-tools config
BACKUP_DIR="$BACKUP_DIR"
RETENTION_DAYS=${RETENTION_DAYS:-7}
LOGIN_PATH="$LOGIN_PATH"
BACKUP_METHOD="${BACKUP_METHOD:-xtrabackup}"
COMPRESS_ALGO="zstd"

# Space management
SPACE_AUTO_CLEANUP=1
SPACE_WARNING_PERCENT=70
SPACE_CRITICAL_PERCENT=90
SPACE_MIN_FREE_GB=5
CLEAN_KEEP_MIN=2
INITCONF
        umask "$old_umask"
        chmod 644 "$CONFIG_FILE" 2>/dev/null || true
        log INFO "✅ Configuration saved to $CONFIG_FILE"
    fi

    log INFO "✅ Initialization complete"
    log INFO "   Backup directory: $BACKUP_DIR"
    log INFO "   Config file: $CONFIG_FILE"
    add_summary "Initialized login-path: $LOGIN_PATH; backup_dir: $BACKUP_DIR"
}




# ========================== Backup Functions ==========================

backup() {
    local backup_type="${1:-full}"
    local db_filter="${2:-}"
    
    need_tooling
    require_login
    ensure_compression_tools
    check_key_perms
    acquire_lock 300 "backup-$backup_type"  # ← Updated with operation name

    # Smart space management: check space, auto-cleanup if needed
    debug "Estimating backup size..."
    local estimated_size
    estimated_size=$(estimate_backup_size) || { err "Failed to estimate backup size"; }
    debug "Estimated size: ${estimated_size}MB"

    debug "Checking space for backup..."
    ensure_space_for_backup "$estimated_size" 4
    debug "Space check passed"

    if [[ -n "$db_filter" && "$backup_type" != "logical" && "$BACKUP_METHOD" == "xtrabackup" ]]; then
        warn "Database filter is only supported for mysqldump; ignoring filter for XtraBackup."
    fi

    case "$backup_type" in
        full)
            if [[ "$BACKUP_METHOD" == "xtrabackup" && -n "$XTRABACKUP" ]]; then
                backup_xtra_full
            else
                backup_full "$db_filter"
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
            backup_full "$db_filter"
            ;;
        *)
            err "Unknown backup type: $backup_type (use: full, incremental, logical)"
            ;;
    esac
}

backup_xtra_full() {
    OPERATION_IN_PROGRESS="xtrabackup-full"

    local ts="$(date +'%Y-%m-%d-%H-%M-%S')"
    local backup_dir="$BACKUP_DIR/xtra-full-$ts"

    log INFO "Starting XtraBackup full backup..."

    # Pre-checks
    local estimated_size
    estimated_size=$(estimate_backup_size)
    check_backup_space "$BACKUP_DIR" "$estimated_size" 4
    cleanup_old_backups
    mkdir -p "$backup_dir"

    # Build command: --defaults-file must come first, and only once
    read -r -a auth_args <<<"$(mysql_auth_args)"  # typically ["--defaults-file=/etc/db-tools.my.cnf"]
    local xtra_cmd=("$XTRABACKUP")
    xtra_cmd+=("${auth_args[@]}")                 # ← FIRST
    xtra_cmd+=(--backup --target-dir="$backup_dir" --parallel="$XTRABACKUP_PARALLEL")

    # Optional compression
    if [[ "$XTRABACKUP_COMPRESS" == "1" ]]; then
        case "$COMPRESS_ALGO" in
            zstd)
                if have zstd; then
                    xtra_cmd+=(--compress=zstd --compress-threads="$XTRABACKUP_COMPRESS_THREADS")
                fi
                ;;
            *)
                if have qpress; then
                    xtra_cmd+=(--compress --compress-threads="$XTRABACKUP_COMPRESS_THREADS")
                fi
                ;;
        esac
    fi

    # (Optional) You can add --host/--port/--user only if NOT using --defaults-file.
    # Since mysql_auth_args provided --defaults-file, skip adding host/user/port.

    log INFO "Executing: ${xtra_cmd[*]}"

    if "${xtra_cmd[@]}" 2>"$backup_dir/xtrabackup.log"; then
        {
            echo "timestamp=$ts"
            echo "backup_type=xtrabackup-full"
            echo "xtrabackup_version=$($XTRABACKUP --version 2>&1 | head -1)"
            echo "server_version=$("$MYSQL" --login-path="$LOGIN_PATH" -N -e 'SELECT VERSION();')"
            echo "compression_algo=$COMPRESS_ALGO"
            echo "compression_level=$COMPRESS_LEVEL"
            echo "parallel_jobs=$PARALLEL_JOBS"
            echo "backup_method=$BACKUP_METHOD"
            [[ "$ENCRYPT_BACKUPS" == "1" ]] && echo "encryption_cipher=aes-256-gcm"
            local gtid_mode
            gtid_mode=$("$MYSQL" --login-path="$LOGIN_PATH" -N -e "SHOW VARIABLES LIKE 'gtid_mode';" 2>/dev/null | awk '{print $2}' || echo "OFF")
            echo "gtid_mode=$gtid_mode"
            if [[ "$gtid_mode" == "ON" ]]; then
                local gtid_executed
                gtid_executed=$("$MYSQL" --login-path="$LOGIN_PATH" -N -e "SELECT @@GLOBAL.gtid_executed;" 2>/dev/null || echo "")
                echo "gtid_executed=$gtid_executed"
            fi
            [[ -f "$backup_dir/xtrabackup_checkpoints" ]] && grep "^to_lsn" "$backup_dir/xtrabackup_checkpoints"
            if [[ -f "$backup_dir/xtrabackup_binlog_info" ]]; then
                local binlog_info
                binlog_info=$(cat "$backup_dir/xtrabackup_binlog_info")
                echo "binlog_file=$(echo "$binlog_info" | awk '{print $1}')"
                echo "binlog_pos=$(echo "$binlog_info" | awk '{print $2}')"
            fi
        } > "$BACKUP_DIR/backup-$ts.meta"

        # Package (don’t recompress page files; just tar)
        log INFO "Compressing backup..."
        if [[ "$ENCRYPT_BACKUPS" == "1" ]]; then
            tar -cf - -C "$BACKUP_DIR" "xtra-full-$ts" \
              | encrypt_if_enabled > "$BACKUP_DIR/xtra-full-$ts.tar.enc.partial"
            mv "$BACKUP_DIR/xtra-full-$ts.tar.enc.partial" "$BACKUP_DIR/xtra-full-$ts.tar.enc"
            rm -rf "$backup_dir"
            backup_dir="$BACKUP_DIR/xtra-full-$ts.tar.enc"
        else
            tar -cf "$BACKUP_DIR/xtra-full-$ts.tar.partial" -C "$BACKUP_DIR" "xtra-full-$ts"
            mv "$BACKUP_DIR/xtra-full-$ts.tar.partial" "$BACKUP_DIR/xtra-full-$ts.tar"
            rm -rf "$backup_dir"
            backup_dir="$BACKUP_DIR/xtra-full-$ts.tar"
        fi

        create_checksum "$backup_dir"
        log INFO "✅ XtraBackup full backup complete: $(basename "$backup_dir")"
        add_summary "XtraBackup full backup: $(basename "$backup_dir")"
        notify "Backup completed successfully" "XtraBackup full backup completed" "info"
        OPERATION_IN_PROGRESS=""
        return 0
    else
        OPERATION_IN_PROGRESS=""
        err "XtraBackup full backup failed. Check $backup_dir/xtrabackup.log"
    fi
}

backup_xtra_incremental() {
    OPERATION_IN_PROGRESS="xtrabackup-incremental"

    local ts="$(date +'%Y-%m-%d-%H-%M-%S')"

    # Locate latest full backup bundle
    local base_backup
    base_backup=$(find "$BACKUP_DIR" -maxdepth 1 \( -name "xtra-full-*.tar" -o -name "xtra-full-*.tar.enc" -o -name "xtra-full-*.tar.gz" \) 2>/dev/null | sort -r | head -1)
    if [[ -z "$base_backup" ]]; then
        warn "No full XtraBackup found, creating full backup instead"
        backup_xtra_full
        return
    fi

    log INFO "Starting XtraBackup incremental backup..."
    log INFO "Base backup: $(basename "$base_backup")"

    # Extract base to a temp dir
    local temp_base="$BACKUP_DIR/.xtra_base_$$"
    mkdir -p "$temp_base"
    stack_trap "rm -rf '$temp_base'" EXIT

    log INFO "Extracting base backup for incremental..."
    if [[ "$base_backup" =~ \.enc$ ]]; then
        decrypt_if_encrypted "$base_backup" < "$base_backup" | tar -xf - -C "$temp_base"
    else
        tar -xf "$base_backup" -C "$temp_base"
    fi

    local base_dir
    base_dir=$(find "$temp_base" -maxdepth 1 -type d -name "xtra-full-*" | head -1)
    [[ -z "$base_dir" ]] && err "Cannot find extracted base backup"

    local backup_dir="$BACKUP_DIR/xtra-incr-$ts"
    mkdir -p "$backup_dir"

    # Build command with correct ordering
    read -r -a auth_args <<<"$(mysql_auth_args)"
    local xtra_cmd=("$XTRABACKUP")
    xtra_cmd+=("${auth_args[@]}")   # --defaults-file first (and only once)
    xtra_cmd+=(--backup --target-dir="$backup_dir" --incremental-basedir="$base_dir" --parallel="$XTRABACKUP_PARALLEL")

    # Optional compression
    if [[ "$XTRABACKUP_COMPRESS" == "1" ]]; then
        case "$COMPRESS_ALGO" in
            zstd)
                if have zstd; then
                    xtra_cmd+=(--compress=zstd --compress-threads="$XTRABACKUP_COMPRESS_THREADS")
                fi
                ;;
            *)
                if have qpress; then
                    xtra_cmd+=(--compress --compress-threads="$XTRABACKUP_COMPRESS_THREADS")
                fi
                ;;
        esac
    fi

    log INFO "Executing: ${xtra_cmd[*]}"

    if "${xtra_cmd[@]}" 2>"$backup_dir/xtrabackup.log"; then
        {
            echo "timestamp=$ts"
            echo "backup_type=xtrabackup-incremental"
            echo "base_backup=$(basename "$base_backup")"
            echo "xtrabackup_version=$($XTRABACKUP --version 2>&1 | head -1)"
            echo "compression_algo=$COMPRESS_ALGO"
            echo "compression_level=$COMPRESS_LEVEL"
            echo "parallel_jobs=$PARALLEL_JOBS"
            echo "backup_method=$BACKUP_METHOD"
            [[ "$ENCRYPT_BACKUPS" == "1" ]] && echo "encryption_cipher=aes-256-gcm"
            [[ -f "$backup_dir/xtrabackup_checkpoints" ]] && {
                grep "^from_lsn" "$backup_dir/xtrabackup_checkpoints"
                grep "^to_lsn"   "$backup_dir/xtrabackup_checkpoints"
            }
        } > "$BACKUP_DIR/backup-$ts.meta"

        log INFO "Compressing incremental backup..."
        if [[ "$ENCRYPT_BACKUPS" == "1" ]]; then
            tar -cf - -C "$BACKUP_DIR" "xtra-incr-$ts" \
              | encrypt_if_enabled > "$BACKUP_DIR/xtra-incr-$ts.tar.enc.partial"
            mv "$BACKUP_DIR/xtra-incr-$ts.tar.enc.partial" "$BACKUP_DIR/xtra-incr-$ts.tar.enc"
            rm -rf "$backup_dir"
            create_checksum "$BACKUP_DIR/xtra-incr-$ts.tar.enc"
            log INFO "✅ XtraBackup incremental backup complete: xtra-incr-$ts.tar.enc"
        else
            tar -cf "$BACKUP_DIR/xtra-incr-$ts.tar.partial" -C "$BACKUP_DIR" "xtra-incr-$ts"
            mv "$BACKUP_DIR/xtra-incr-$ts.tar.partial" "$BACKUP_DIR/xtra-incr-$ts.tar"
            rm -rf "$backup_dir"
            create_checksum "$BACKUP_DIR/xtra-incr-$ts.tar"
            log INFO "✅ XtraBackup incremental backup complete: xtra-incr-$ts.tar"
        fi

        add_summary "XtraBackup incremental backup: xtra-incr-$ts"
        notify "Incremental backup completed" "XtraBackup incremental backup completed" "info"
        OPERATION_IN_PROGRESS=""
        return 0
    else
        OPERATION_IN_PROGRESS=""
        err "XtraBackup incremental backup failed. Check $backup_dir/xtrabackup.log"
    fi
}



# ---- full replacement for restore_xtra() ----
restore_xtra() {
    OPERATION_IN_PROGRESS="xtrabackup-restore"

    local backup_file="$1"
    local target_db="${2:-}"
    local until_time="${3:-}"

    log INFO "Restoring from XtraBackup: $(basename "$backup_file")"

    # Disk space & checksum
    check_restore_space "$backup_file" 3
    verify_checksum "$backup_file" || warn "Checksum verification failed"

    # Extract archive to temp
    local temp_restore="$BACKUP_DIR/.xtra_restore_$$"
    mkdir -p "$temp_restore"
    stack_trap "rm -rf '$temp_restore'" EXIT

    log INFO "Extracting backup..."
    if [[ "$backup_file" =~ \.enc$ ]]; then
        decrypt_if_encrypted "$backup_file" < "$backup_file" | tar -xf - -C "$temp_restore"
    else
        tar -xf "$backup_file" -C "$temp_restore"
    fi

    local backup_dir
    backup_dir=$(find "$temp_restore" -maxdepth 1 -type d -name "xtra-*" | head -1)
    [[ -z "$backup_dir" ]] && err "Cannot find extracted backup directory"

    # Build credential args once; reuse for all xtrabackup calls
    local auth_args=()
    read -r -a auth_args <<<"$(mysql_auth_args)"

    # If backup was compressed by xtrabackup (qpress/zstd), decompress on disk
    if [[ -f "$backup_dir/xtrabackup_checkpoints" ]] && grep -q "compressed = 1" "$backup_dir/xtrabackup_checkpoints" 2>/dev/null; then
        log INFO "Decompressing backup pages..."
        "$XTRABACKUP" --decompress \
            --target-dir="$backup_dir" \
            --parallel="$XTRABACKUP_PARALLEL" \
            "${auth_args[@]}" || err "Decompression failed"
        # remove compressed chunks
        find "$backup_dir" \( -name "*.qp" -o -name "*.zst" \) -delete 2>/dev/null || true
    fi

    # Determine base timestamp (from filename) to select incrementals created after it
    local base_ts
    base_ts=$(basename "$backup_file" | grep -oE '[0-9]{4}-[0-9]{2}-[0-9]{2}-[0-9]{2}-[0-9]{2}-[0-9]{2}' || true)

    # Prepare base (apply-log-only if we will merge incrementals)
    log INFO "Preparing base backup..."
    "$XTRABACKUP" --prepare \
        --apply-log-only \
        --target-dir="$backup_dir" \
        --use-memory="$XTRABACKUP_MEMORY" \
        "${auth_args[@]}" || err "Base prepare failed"

    # Find and apply incrementals created AFTER the base backup
    local incrementals=()
    if [[ -n "$base_ts" ]]; then
        while IFS= read -r incr; do
            local incr_ts
            incr_ts=$(basename "$incr" | grep -oE '[0-9]{4}-[0-9]{2}-[0-9]{2}-[0-9]{2}-[0-9]{2}-[0-9]{2}' || true)
            [[ -n "$incr_ts" && "$incr_ts" > "$base_ts" ]] && incrementals+=("$incr")
        done < <(find "$BACKUP_DIR" -maxdepth 1 -type f -name "xtra-incr-*.tar.*" 2>/dev/null | sort)
    fi

    if [[ ${#incrementals[@]} -gt 0 ]]; then
        log INFO "Applying ${#incrementals[@]} incremental backup(s)..."
        for incr in "${incrementals[@]}"; do
            log INFO "Incremental: $(basename "$incr")"
            local temp_incr="$temp_restore/incr_$$"
            mkdir -p "$temp_incr"

            # extract incremental
            if [[ "$incr" =~ \.enc$ ]]; then
                decrypt_if_encrypted "$incr" < "$incr" | tar -xf - -C "$temp_incr"
            else
                tar -xf "$incr" -C "$temp_incr"
            fi
            local incr_dir
            incr_dir=$(find "$temp_incr" -maxdepth 1 -type d -name "xtra-incr-*" | head -1)

            # decompress incremental pages if needed
            if [[ -f "$incr_dir/xtrabackup_checkpoints" ]] && grep -q "compressed = 1" "$incr_dir/xtrabackup_checkpoints" 2>/dev/null; then
                "$XTRABACKUP" --decompress \
                    --target-dir="$incr_dir" \
                    --parallel="$XTRABACKUP_PARALLEL" \
                    "${auth_args[@]}" || err "Incremental decompression failed"
                find "$incr_dir" \( -name "*.qp" -o -name "*.zst" \) -delete 2>/dev/null || true
            fi

            # merge incremental into base (apply-log-only)
            "$XTRABACKUP" --prepare \
                --apply-log-only \
                --target-dir="$backup_dir" \
                --incremental-dir="$incr_dir" \
                --use-memory="$XTRABACKUP_MEMORY" \
                "${auth_args[@]}" || err "Incremental apply failed"

            rm -rf "$temp_incr"
        done
    else
        log INFO "No incrementals found to apply"
    fi

    # Final prepare (make backup consistent for copy-back)
    log INFO "Final prepare..."
    "$XTRABACKUP" --prepare \
        --target-dir="$backup_dir" \
        --use-memory="$XTRABACKUP_MEMORY" \
        "${auth_args[@]}" || err "Final prepare failed"

    # === CRITICAL SECTION: stop MySQL and copy back ===
    log WARN "=== CRITICAL: Stopping MySQL for restore ==="
    MYSQL_WAS_STOPPED=1
    stop_mysql || err "Cannot stop MySQL/MariaDB"

    local datadir="/var/lib/mysql"
    local backup_old="$datadir.backup.$(date +%s)"
    DATADIR_BACKUP_PATH="$backup_old"

    log INFO "Backing up current datadir to $backup_old"
    mv "$datadir" "$backup_old" || err "Cannot backup current datadir"
    mkdir -p "$datadir"

    log INFO "Copying files back to datadir (this can take a while)..."
    if ! "$XTRABACKUP" --copy-back \
            --target-dir="$backup_dir" \
            "${auth_args[@]}"; then
        log ERROR "Copy-back failed!"
        log ERROR "Attempting to restore original datadir..."
        rm -rf "$datadir"
        mv "$backup_old" "$datadir"
        DATADIR_BACKUP_PATH=""
        MYSQL_WAS_STOPPED=0
        err "Restore failed - original datadir restored"
    fi

    # Permissions/SELinux
    if chown -R mysql:mysql "$datadir" 2>/dev/null; then
        :
    elif chown -R mariadb:mariadb "$datadir" 2>/dev/null; then
        :
    else
        warn "Could not set ownership to mysql/mysql or mariadb/mariadb"
    fi
    command -v restorecon >/dev/null 2>&1 && restorecon -Rv "$datadir" 2>/dev/null || true

    # Start MySQL again
    log INFO "Starting MySQL..."
    if start_mysql; then
        MYSQL_WAS_STOPPED=0
        DATADIR_BACKUP_PATH=""
        log INFO "✅ XtraBackup restore complete"

        # Optional PITR (until_time) after physical restore
        if [[ -n "$until_time" && -f "$backup_dir/xtrabackup_binlog_info" ]]; then
            local binlog_file binlog_pos
            binlog_file=$(awk '{print $1}' "$backup_dir/xtrabackup_binlog_info")
            binlog_pos=$(awk '{print $2}' "$backup_dir/xtrabackup_binlog_info")
            log INFO "Applying binlogs from $binlog_file:$binlog_pos to $until_time"
            run_pitr_from_position "$binlog_file" "$binlog_pos" "$until_time"
        fi

        log INFO "Old datadir backup retained at: $backup_old (remove after validation)"
        add_summary "XtraBackup restore: Success"
        OPERATION_IN_PROGRESS=""
    else
        err "Cannot start MySQL after restore - CHECK IMMEDIATELY!"
    fi
}


run_pitr_from_position() {
    local start_binlog="$1"
    local start_pos="$2"
    local until_time="$3"
    
    [[ -z "$MYSQLBINLOG" ]] && { warn "mysqlbinlog not available"; return 1; }
    
    # Try to read from binlog index first
    local index="/var/lib/mysql/binlog.index"
    local binlogs=()
    
    if [[ -r "$index" ]]; then
        while IFS= read -r line; do
            line="${line%\"}"; line="${line#\"}"
            [[ -f "$line" ]] && binlogs+=("$line")
        done < "$index"
    else
        warn "Binlog index not readable; falling back to glob"
        mapfile -t binlogs < <(find /var/lib/mysql -name "*.0*" -type f 2>/dev/null | sort)
    fi
    
    [[ ${#binlogs[@]} -gt 0 ]] || { warn "No binlogs found"; return 1; }
    
    # Find starting binlog
    local start_i=-1
    for i in "${!binlogs[@]}"; do
        if [[ "$(basename "${binlogs[$i]}")" == "$start_binlog" ]]; then
            start_i="$i"
            break
        fi
    done
    
    if (( start_i < 0 )); then
        warn "Starting binlog not found: $start_binlog"
        return 1
    fi
    
    log INFO "Applying ${#binlogs[@]} binlog file(s) for PITR (starting at index $start_i)"
    
    local last_i=$((${#binlogs[@]} - 1))
    
    for i in $(seq "$start_i" "$last_i"); do
        local cmd=("$MYSQLBINLOG")
        
        # Start position only for first file
        if [[ $i -eq $start_i && -n "$start_pos" ]]; then
            cmd+=(--start-position="$start_pos")
        fi
        
        # Stop datetime for all files
        [[ -n "$until_time" ]] && cmd+=(--stop-datetime="$until_time")
        
        cmd+=("${binlogs[$i]}")
        
        log INFO "Processing: $(basename "${binlogs[$i]}")"
        
        if ! "${cmd[@]}" | "$MYSQL" --login-path="$LOGIN_PATH"; then
            if [[ -n "$until_time" ]]; then
                debug "Binlog replay stopped (likely reached target time)"
                break
            else
                warn "Error applying ${binlogs[$i]}"
            fi
        fi
    done
    
    log INFO "✅ PITR complete"
}

backup_full() {
    local db_filter="${1:-}"

    OPERATION_IN_PROGRESS="mysqldump-full"
    
    # Ensure mysqldump is available
    [[ -n "$MYSQLDUMP" ]] || err "mysqldump not found; set BACKUP_METHOD=xtrabackup or install mysqldump"

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

    if [[ -n "$db_filter" ]]; then
        validate_identifier "$db_filter" "database" || err "Invalid database name: $db_filter"
        if ! printf '%s\n' "${DBS[@]}" | grep -qx "$db_filter"; then
            err "Database not found: $db_filter"
        fi
        DBS=("$db_filter")
    fi
    
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
            # Stream-decompress and read only the first few lines (head will SIGPIPE the decompressor)
            local decomp_cmd="$(decompressor "$out")"
            local header=""
            header=$(
                set +e +o pipefail
                if [[ "$ENCRYPT_BACKUPS" == "1" ]]; then
                    decrypt_if_encrypted "$out" < "$out" | $decomp_cmd 2>/dev/null | head -n 50
                else
                    $decomp_cmd < "$out" 2>/dev/null | head -n 50
                fi
            ) || true
            if ! printf '%s' "$header" | grep -q "^-- MySQL dump"; then
                warn "Backup validation failed: $db"
                return 1
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
          ((job_num++)) || true

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
        
        # Count results from status files more reliably
        completed=0
        errors=0
        for sf in "$status_dir"/job_*.status; do
            [[ -f "$sf" ]] || continue
            if grep -q "ok" "$sf" 2>/dev/null; then
                ((completed++)) || true
            else
                ((errors++)) || true
            fi
        done

        # Show final progress
        show_progress "$completed" "${#DBS[@]}" "Backup"
    else
        for db in "${DBS[@]}"; do
            if dump_one "$db"; then
                ((completed++)) || true
            else
                ((errors++)) || true
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
        EXPECTED_ERROR_EXIT=1  # Partial success, not an emergency
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

    # Safe metadata parsing (don't source untrusted files)
    local binlog_file="" binlog_pos=""
    while IFS='=' read -r key value || [[ -n "$key" ]]; do
        case "$key" in
            binlog_file) binlog_file="$value" ;;
            binlog_pos) binlog_pos="$value" ;;
        esac
    done < "$last_meta"

    if [[ -z "${binlog_file:-}" ]]; then
        err "Last backup metadata incomplete, cannot perform incremental backup"
    fi
    
    local ts="$(date +'%Y-%m-%d-%H-%M-%S')"
    local comp="$(compressor)"
    local ext="$(get_binlog_extension)"
    
    log INFO "Starting incremental backup from $binlog_file:$binlog_pos"
    
    local output="$BACKUP_DIR/incremental-${ts}.${ext}"
    [[ "$ENCRYPT_BACKUPS" == "1" ]] && output="${output}.enc"
    
    # Build list of binlogs from index
    local index="/var/lib/mysql/binlog.index"
    if [[ ! -r "$index" ]]; then
        err "Binlog index not readable: $index (is binary logging enabled?)"
    fi
    
    # Read binlog list, stripping quotes
    local binlogs=()
    while IFS= read -r line; do
        line="${line%\"}"; line="${line#\"}"
        [[ -f "$line" ]] && binlogs+=("$line")
    done < "$index"
    
    [[ ${#binlogs[@]} -gt 0 ]] || err "No binlogs listed in $index"
    
    # Find start index
    local start_i=-1
    for i in "${!binlogs[@]}"; do
        if [[ "$(basename "${binlogs[$i]}")" == "$binlog_file" ]]; then
            start_i="$i"
            break
        fi
    done
    
    if (( start_i < 0 )); then
        err "Start binlog $binlog_file not found in index"
    fi
    
    log INFO "Capturing ${#binlogs[@]} binlog file(s) from position $start_i"
    
    {
        "$MYSQLBINLOG" --start-position="$binlog_pos" "${binlogs[$start_i]}" || exit 1
        
        if (( start_i + 1 <= ${#binlogs[@]} - 1 )); then
            "$MYSQLBINLOG" "${binlogs[@]:$((start_i+1))}" || exit 1
        fi
    } | $comp | encrypt_if_enabled > "$output" || err "Incremental backup failed"
    
    create_checksum "$output"
    log INFO "✅ Incremental backup created: $(basename "$output")"
    add_summary "Incremental backup: $(basename "$output")"
}


cleanup_old_backups() {
  # Age-based pruning for dumps, binlog bundles, and checksums
  [[ -d "$BACKUP_DIR" ]] || { log INFO "Backup dir not found: $BACKUP_DIR"; return 0; }

  log INFO "Pruning items older than ${RETENTION_DAYS} day(s)… (dry_run=$DRY_RUN)"
  
  # More portable: two-pass approach
  local files_to_delete=()
  while IFS= read -r f; do
    files_to_delete+=("$f")
  done < <(find "$BACKUP_DIR" -maxdepth 1 -type f \
            \( -name "*.sql.*" -o -name "*.binlog.*" -o -name "*.sha256" \) \
            -mtime +"${RETENTION_DAYS}" 2>/dev/null)
  
  if (( DRY_RUN == 1 )); then
    for f in "${files_to_delete[@]}"; do
      log INFO "DRY-RUN: Would delete $(basename "$f")"
    done
  else
    for f in "${files_to_delete[@]}"; do
      log INFO "Deleting: $(basename "$f")"
      rm -f "$f"
    done
  fi
}



# ========================== Verify Functions ==========================

verify() {
    require_login
    shopt -s nullglob
    
    local files=("$BACKUP_DIR"/*.sql.* "$BACKUP_DIR"/*.binlog.* "$BACKUP_DIR"/*.tar.*)
    [[ ${#files[@]} -gt 0 ]] || { log INFO "No backups found in $BACKUP_DIR"; return 0; }
    
    log INFO "Verifying ${#files[@]} backup file(s)..."
    
    local ok=0 bad=0 warn_count=0
    local idx=0
    
    start_spinner "Verifying backups"

    for f in "${files[@]}"; do
        ((idx++))
        show_progress "$idx" "${#files[@]}" "Verify"
        
        local base="$(basename "$f")"
        [[ "$base" =~ \.(sha256|meta)$ ]] && continue
        
        # Check checksum if present
        if ! verify_checksum "$f"; then
            ((warn_count++))
        fi
        
        # Tar bundles (xtrabackup): try listing
        if [[ "$f" =~ \.tar(\.enc)?$ ]]; then
            if [[ "$f" =~ \.enc$ ]]; then
                if decrypt_if_encrypted "$f" < "$f" | tar -tf - >/dev/null 2>&1; then
                    ((ok++)); debug "OK(tar,enc): $base"
                else
                    ((bad++)); warn "CORRUPT (tar,enc): $base"; continue
                fi
            else
                if tar -tf "$f" >/dev/null 2>&1; then
                    ((ok++)); debug "OK(tar): $base"
                else
                    ((bad++)); warn "CORRUPT (tar): $base"; continue
                fi
            fi
            continue
        fi
        
        # Streamed (sql/binlog) bundles: decrypt → decompress to /dev/null
        local decomp_cmd="$(decompressor "$f")"
        if [[ "$f" =~ \.enc$ ]]; then
            if decrypt_if_encrypted "$f" < "$f" | $decomp_cmd > /dev/null 2>&1; then
                ((ok++)); debug "OK(enc): $base"
            else
                ((bad++)); warn "CORRUPT (enc): $base"; continue
            fi
        else
            if $decomp_cmd < "$f" > /dev/null 2>&1; then
                ((ok++)); debug "OK: $base"
            else
                ((bad++)); warn "CORRUPT: $base"; continue
            fi
        fi
        
        # Optional: quick header check for SQL dumps
        if [[ "$base" == *.sql.* || "$base" == *.sql.*.enc ]]; then
            local hdr_ok=0
            if [[ "$f" =~ \.enc$ ]]; then
                if decrypt_if_encrypted "$f" < "$f" | $decomp_cmd | head -n 50 | grep -q "^-- MySQL dump" 2>/dev/null; then
                    hdr_ok=1
                fi
            else
                if $decomp_cmd < "$f" | head -n 50 | grep -q "^-- MySQL dump" 2>/dev/null; then
                    hdr_ok=1
                fi
            fi
            (( hdr_ok == 1 )) || { ((warn_count++)); debug "Header check failed: $base"; }
        fi
        
        # Metadata presence
        local ts="$(dump_ts_from_name "$base")"
        if [[ -n "$ts" ]]; then
            local meta="$BACKUP_DIR/backup-$ts.meta"
            [[ -f "$meta" ]] || { ((warn_count++)); debug "Missing metadata: $base"; }
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
    
    # Safety check: Confirmation
    if [[ "${FORCE_RESTORE:-0}" != "1" ]]; then
        warn "WARNING: Restore operation may overwrite existing data!"
        if [[ -t 0 ]]; then
            read -r -p "Are you sure you want to continue? [y/N] " confirm
            [[ "$confirm" =~ ^[Yy]$ ]] || { log INFO "Restore cancelled."; return 0; }
        else
            err "Non-interactive restore requires FORCE_RESTORE=1 environment variable."
        fi
    fi

    acquire_lock 300 "restore"
    
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

    # Verify integrity quickly before doing any changes
    verify_checksum "$file" || warn "Checksum verification failed"

    local decomp_cmd
    decomp_cmd="$(decompressor "$file")"
    if [[ "$file" =~ \.enc$ ]]; then
      decrypt_if_encrypted "$file" < "$file" | $decomp_cmd >/dev/null 2>&1 \
        || err "Backup file appears corrupt (enc): $file"
    else
      $decomp_cmd < "$file" >/dev/null 2>&1 \
        || err "Backup file appears corrupt: $file"
    fi

    # Derive source DB from filename and pick destination
    local base src_db dest_db dump_ts
    base="$(basename "$file")"
    src_db="${base%%-[0-9][0-9][0-9][0-9]-*}"
    [[ -n "$src_db" ]] || err "Cannot determine source database from filename"

    dest_db="${target_db_opt:-$src_db}"
    dump_ts="$(dump_ts_from_name "$base")"

    # Validate database names to prevent SQL injection
    validate_identifier "$src_db" "database" || err "Invalid source database name: $src_db"
    validate_identifier "$dest_db" "database" || err "Invalid target database name: $dest_db"

    log INFO "Source DB: $src_db → Target DB: $dest_db"

    # Optional DROP safety
    if [[ "${DROP_FIRST:-0}" == "1" ]]; then
        warn "DROP_FIRST=1: Dropping database \`$dest_db\`"
        "$MYSQL" --login-path="$LOGIN_PATH" -e "DROP DATABASE IF EXISTS \`$dest_db\`;"
    fi

    # Always ensure DB exists with sane defaults
    "$MYSQL" --login-path="$LOGIN_PATH" -e \
      "CREATE DATABASE IF NOT EXISTS \`$dest_db\` CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;"

    # Build the import pipeline:
    # decrypt → decompress → sanitize (neutralize DEFINER, disable binlog, relax FKs) → mysql
    # If src == dest, use dump as-is; if renamed, strip CREATE/USE and target explicitly.
    if [[ "$dest_db" == "$src_db" ]]; then
        if [[ "$file" =~ \.enc$ ]]; then
            decrypt_if_encrypted "$file" < "$file" \
              | $decomp_cmd \
              | sanitize_dump_stream \
              | "$MYSQL" --login-path="$LOGIN_PATH"
        else
            $decomp_cmd < "$file" \
              | sanitize_dump_stream \
              | "$MYSQL" --login-path="$LOGIN_PATH"
        fi
    else
        warn "Renaming database on restore: $src_db → $dest_db"
        if [[ "$file" =~ \.enc$ ]]; then
            decrypt_if_encrypted "$file" < "$file" \
              | $decomp_cmd \
              | sed -E '/^CREATE DATABASE/d; /^USE `/d' \
              | sanitize_dump_stream \
              | "$MYSQL" --login-path="$LOGIN_PATH" -D "$dest_db"
        else
            $decomp_cmd < "$file" \
              | sed -E '/^CREATE DATABASE/d; /^USE `/d' \
              | sanitize_dump_stream \
              | "$MYSQL" --login-path="$LOGIN_PATH" -D "$dest_db"
        fi
    fi

    # Optional PITR after logical restore
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

    # Safe metadata parsing (don't source untrusted files)
    local binlog_file="" binlog_pos=""
    while IFS='=' read -r key value || [[ -n "$key" ]]; do
        case "$key" in
            binlog_file) binlog_file="$value" ;;
            binlog_pos) binlog_pos="$value" ;;
        esac
    done < "$meta"

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
    if [[ -n "$db_filter" ]]; then
        log INFO "Database filter: $db_filter"
        warn "PITR with --database filters cross-DB transactions; integrity depends on workload."
    fi

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
    
    # Disk space - comprehensive check
    local available_mb
    available_mb=$(df -BM "$BACKUP_DIR" 2>/dev/null | awk 'NR==2 {print $4}' | sed 's/M//' || echo "0")
    [[ "$available_mb" =~ ^[0-9]+$ ]] || available_mb=0
    local available_gb=$((available_mb / 1024))
    local usage_percent
    usage_percent=$(get_disk_usage_percent 2>/dev/null || echo "0")
    [[ "$usage_percent" =~ ^[0-9]+$ ]] || usage_percent=0
    local total_backup_mb
    total_backup_mb=$(get_backup_total_size_mb 2>/dev/null || echo "0")
    [[ "$total_backup_mb" =~ ^[0-9]+$ ]] || total_backup_mb=0
    local total_backup_gb=$((total_backup_mb / 1024))

    if (( usage_percent >= ${SPACE_CRITICAL_PERCENT:-90} )); then
        echo "❌ Disk Space: ${available_gb}GB free, ${usage_percent}% used (CRITICAL!)"
        ((status++))
    elif (( usage_percent >= ${SPACE_WARNING_PERCENT:-70} )); then
        echo "⚠️  Disk Space: ${available_gb}GB free, ${usage_percent}% used (warning)"
        ((status++))
    elif (( available_gb < ${SPACE_MIN_FREE_GB:-5} )); then
        echo "⚠️  Disk Space: ${available_gb}GB free (below ${SPACE_MIN_FREE_GB:-5}GB minimum)"
        ((status++))
    else
        echo "✅ Disk Space: ${available_gb}GB free, ${usage_percent}% used"
    fi

    # Backup size info
    echo "ℹ️  Total Backup Size: ${total_backup_gb}GB (${total_backup_mb}MB)"
    if [[ "${SPACE_MAX_USAGE_GB:-0}" -gt 0 ]] 2>/dev/null; then
        if (( total_backup_gb >= SPACE_MAX_USAGE_GB )); then
            echo "⚠️  Backup Size Limit: ${total_backup_gb}GB / ${SPACE_MAX_USAGE_GB}GB (exceeded!)"
            ((status++))
        else
            echo "ℹ️  Backup Size Limit: ${total_backup_gb}GB / ${SPACE_MAX_USAGE_GB}GB"
        fi
    fi
    
    # Last backup
    local last_backup
    last_backup=$(find "$BACKUP_DIR" -name "*.sql.*" -o -name "*.tar.*" -type f ! -name "*.sha256" -printf '%T@ %p\n' 2>/dev/null | sort -n | tail -1 | cut -d' ' -f2- || true)
    if [[ -n "$last_backup" ]]; then
        # BSD-safe stat
        local last_mtime
        if last_mtime=$(stat -c %Y "$last_backup" 2>/dev/null); then
            :  # GNU stat succeeded
        else
            last_mtime=$(stat -f %m "$last_backup" 2>/dev/null || echo 0)
        fi
        
        local age=$(( ($(date +%s) - last_mtime) / 3600 ))
        
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
    
    log INFO "=== Database Tuning Advisor ==="
    
    # 1. MySQLTuner
    # Calculate total memory for --forcemem if needed, or omit it.
    # mysqltuner requires a value for --forcemem (MB).
    local total_mem_mb
    total_mem_mb=$(free -m 2>/dev/null | awk '/Mem:/ {print $2}' || echo "")
    
    local tuner_args=(--nocolor)
    # Only add forcemem if we successfully got memory size
    if [[ -n "$total_mem_mb" && "$total_mem_mb" -gt 0 ]]; then
        tuner_args+=(--forcemem "$total_mem_mb")
    fi

    if have mysqltuner; then
        log INFO "Running MySQLTuner..."
        # Remove --silent to see output, or keep it if user wants less noise. 
        # But usually tuning output is the point. 
        # The user's log showed help text because of invalid args.
        mysqltuner "${tuner_args[@]}" 2>/dev/null || true
    else
        warn "mysqltuner not found. Attempting to download..."
        
        # Determine install location
        local tuner_path="mysqltuner.pl"
        local install_mode=0
        
        if [[ "$EUID" -eq 0 ]]; then
            tuner_path="/usr/local/bin/mysqltuner"
            install_mode=1
        fi

        if curl -fsSL http://mysqltuner.pl/ -o "$tuner_path"; then
            if (( install_mode )); then
                chmod +x "$tuner_path"
                log INFO "✅ Installed mysqltuner to $tuner_path"
                "$tuner_path" "${tuner_args[@]}" 2>/dev/null || true
            else
                log INFO "Running temporary mysqltuner.pl..."
                perl "$tuner_path" "${tuner_args[@]}" 2>/dev/null || true
                rm "$tuner_path"
            fi
        else
            warn "Failed to download mysqltuner. Install manually: apt-get install mysqltuner"
        fi
    fi
    
    echo
    
    # 2. Percona Toolkit (pt-variable-advisor)
    if have pt-variable-advisor; then
        log INFO "Running Percona pt-variable-advisor..."
        # pt-variable-advisor reads from STDIN if no DSN is provided, but we need to be explicit
        # The previous error "Unknown MySQL server host '-'" suggests it didn't like the pipe or arguments
        # We will save variables to a temp file and pass that instead
        
        local vars_file
        vars_file=$(mktemp)
        "$MYSQL" --login-path="$LOGIN_PATH" -e "SHOW VARIABLES" > "$vars_file"
        
        pt-variable-advisor "$vars_file" || true
        
        rm -f "$vars_file"
    else
        warn "pt-variable-advisor not found (install percona-toolkit)"
    fi

    echo
    
    # 3. Percona Configuration Wizard (Online)
    log INFO "For advanced configuration generation, visit Percona Configuration Wizard:"
    log INFO "https://tools.percona.com/wizard"
}

# ========================== Maintenance ==========================

maintain() {
    require_login
    acquire_lock 600 "maintenance"

    local mode="quick"
    local force=0 safe_flag=0
    # args: maintain [quick|full] [--safe] [--force]
    if [[ -n "${1:-}" ]]; then
      case "$1" in
        quick|full) mode="$1"; shift ;;
      esac
    fi
    while [[ -n "${1:-}" ]]; do
      case "$1" in
        --safe) safe_flag=1 ;;
        --force) force=1 ;;
        *) warn "Unknown option to maintain: $1" ;;
      esac
      shift || true
    done

    # Decide if we should run safe mode
    local use_safe=0
    if (( safe_flag )); then
      use_safe=1
    elif (( force )); then
      use_safe=0
    elif should_safe_mode; then
      use_safe=1
    fi

    log INFO "Running maintenance mode: $mode (safe=$use_safe, force=$force)"

    mapfile -t tables < <(
        "$MYSQL" --login-path="$LOGIN_PATH" -N -e "
            SELECT CONCAT(TABLE_SCHEMA,'.',TABLE_NAME)
            FROM information_schema.TABLES
            WHERE TABLE_SCHEMA NOT IN ('mysql','information_schema','performance_schema','sys')
              AND TABLE_TYPE='BASE TABLE'
            ORDER BY TABLE_SCHEMA, TABLE_NAME;"
    )

    [[ ${#tables[@]} -gt 0 ]] || { log INFO "No tables found"; return 0; }

    # Always ANALYZE
    log INFO "Analyzing ${#tables[@]} table(s)..."
    local idx=0
    for t in "${tables[@]}"; do
        ((idx++))
        # Validate table name to prevent SQL injection (even though from information_schema)
        if ! validate_identifier "$t" "table"; then
            warn "Skipping invalid table name: $t"
            continue
        fi
        # Split schema.table for proper backtick escaping
        local schema_name table_name
        schema_name="${t%%.*}"
        table_name="${t#*.}"
        show_progress "$idx" "${#tables[@]}" "ANALYZE"
        "$MYSQL" --login-path="$LOGIN_PATH" -e "ANALYZE TABLE \`${schema_name}\`.\`${table_name}\`;" >/dev/null 2>&1 || warn "ANALYZE failed: $t"
    done

    if [[ "$mode" == "full" ]]; then
        if (( use_safe )); then
            warn "SAFE mode active: skipping OPTIMIZE TABLE (risk conditions detected)"
        else
            # Optional: skip huge tables individually if space is marginal
            local free_mb
            free_mb=$(get_free_mb "/var/lib/mysql")
            log INFO "Starting OPTIMIZE (free space: ${free_mb}MB)"
            idx=0
            for t in "${tables[@]}"; do
                ((idx++))
                # Validate table name (already validated in ANALYZE loop, but be defensive)
                if ! validate_identifier "$t" "table"; then
                    warn "Skipping invalid table name: $t"
                    continue
                fi
                show_progress "$idx" "${#tables[@]}" "OPTIMIZE"
                # Check per-table size; skip if free space < ratio * table size
                # Split schema.table for proper escaping
                local schema_name table_name
                schema_name="${t%%.*}"
                table_name="${t#*.}"
                local tmb
                tmb=$("$MYSQL" --login-path="$LOGIN_PATH" -N -e "
                  SELECT COALESCE(ROUND((data_length+index_length)/1024/1024),0)
                  FROM information_schema.TABLES
                  WHERE TABLE_SCHEMA='${schema_name}' AND TABLE_NAME='${table_name}';" 2>/dev/null | awk '{print int($1)}')
                local need_mb
                need_mb=$(awk -v r="${SAFE_MIN_FREE_RATIO:-2.0}" -v t="${tmb:-0}" 'BEGIN { printf "%d", r * t + 0.5 }')
                # If per-table check fails, skip that table
                if (( free_mb > need_mb )); then
                    "$MYSQL" --login-path="$LOGIN_PATH" -e "OPTIMIZE TABLE \`${schema_name}\`.\`${table_name}\`;" >/dev/null 2>&1 || warn "OPTIMIZE failed: $t"
                else
                    warn "Skipping OPTIMIZE for ${t} (free ${free_mb}MB < need ${need_mb}MB)"
                fi
            done
        fi
    fi

    log INFO "✅ Maintenance complete"
    add_summary "Maintenance (${mode}${use_safe:+,safe}): ${#tables[@]} tables processed"
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
    # Portable approach: use while loop instead of xargs -d (GNU-specific)
    files=()
    while IFS= read -r filepath; do
        [[ -z "$filepath" ]] && continue
        local mtime
        # Try GNU stat first, then BSD stat
        if mtime=$(stat -c "%Y" "$filepath" 2>/dev/null); then
            :
        else
            mtime=$(stat -f "%m" "$filepath" 2>/dev/null || echo "0")
        fi
        echo "$mtime $filepath"
    done < <(printf "%s" "${bydb[$db]}" | sed '/^$/d') | sort -rn | while IFS= read -r line; do
        echo "${line#* }"
    done | while IFS= read -r sorted_file; do
        [[ -n "$sorted_file" ]] && files+=("$sorted_file")
    done
    # Re-read into array (subshell issue workaround)
    files=()
    while IFS= read -r sorted_file; do
        [[ -n "$sorted_file" ]] && files+=("$sorted_file")
    done < <(
        while IFS= read -r filepath; do
            [[ -z "$filepath" ]] && continue
            local mtime
            if mtime=$(stat -c "%Y" "$filepath" 2>/dev/null); then
                :
            else
                mtime=$(stat -f "%m" "$filepath" 2>/dev/null || echo "0")
            fi
            echo "$mtime $filepath"
        done < <(printf "%s" "${bydb[$db]}" | sed '/^$/d') | sort -rn | cut -d' ' -f2-
    )
    
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

# Compression (zstd, pigz, gzip, xz)
COMPRESS_ALGO="zstd"
COMPRESS_LEVEL=6

# XtraBackup (physical backups - faster for InnoDB)
BACKUP_METHOD="xtrabackup"  # xtrabackup or mysqldump
XTRABACKUP_ENABLED=1
XTRABACKUP_PARALLEL=4
XTRABACKUP_COMPRESS=1
XTRABACKUP_COMPRESS_THREADS=4
XTRABACKUP_MEMORY="1G"  # Memory for prepare phase

# GFS Rotation
CLEAN_KEEP_MIN=2

# Smart Space Management
SPACE_AUTO_CLEANUP=1              # Auto-remove old backups if space is low
SPACE_MAX_USAGE_GB=0              # Max total backup size in GB (0=unlimited)
SPACE_MAX_USAGE_PERCENT=80        # Max disk usage % before warning/cleanup
SPACE_WARNING_PERCENT=70          # Send warning when disk usage exceeds this
SPACE_CRITICAL_PERCENT=90         # Critical alert threshold
SPACE_MIN_FREE_GB=5               # Always keep at least this much free space
SPACE_CLEANUP_PARTIAL=1           # Clean orphaned .partial files on startup

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
  backup [full|incremental] [db]  Create backup (XtraBackup by default)
  backup logical [db]             Create logical backup (mysqldump)
  verify                        Verify backup integrity with checksums
  restore <DB|ALL|file>         Restore (auto-detects backup type)
  list                          List all available backups

  health                        Run system health check
  sizes                         Show database and table sizes
  space                         Show backup space usage summary
  tune                          Run tuning advisors (mysqltuner, pt-variable-advisor)
  tune-parallel                 Show auto-tuned parallel jobs count
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
  COMPRESS_ALGO                 zstd, pigz, gzip, xz (default: zstd)
  ENCRYPT_BACKUPS               0=off, 1=on (default: 0)
  CHECKSUM_ENABLED              0=off, 1=on (default: 1)
  NOTIFY_EMAIL                  Email for notifications
  NOTIFY_WEBHOOK                Webhook URL for notifications
  LOG_LEVEL                     DEBUG, INFO, WARN, ERROR (default: INFO)
  DRY_RUN                       0=off, 1=on (default: 0)

Space Management:
  SPACE_AUTO_CLEANUP            Auto-cleanup old backups if space low (default: 1)
  SPACE_MAX_USAGE_GB            Max total backup size in GB, 0=unlimited (default: 0)
  SPACE_MAX_USAGE_PERCENT       Max disk usage % for backups (default: 80)
  SPACE_WARNING_PERCENT         Warning threshold % (default: 70)
  SPACE_CRITICAL_PERCENT        Critical threshold % (default: 90)
  SPACE_MIN_FREE_GB             Minimum free GB to maintain (default: 5)
  CLEAN_KEEP_MIN                Minimum backups to keep per DB (default: 2)

PITR (Point-in-Time Recovery):
  UNTIL_TIME="YYYY-MM-DD HH:MM:SS"
  END_POS=<position>
  
Examples:
  # Initial setup
  $0 init
  
  # Create full backup
  $0 backup

  # Create logical backup for a single database
  $0 backup logical mydb
  
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

Version: $DB_TOOLS_VERSION
EOF
}

# ========================== Status Check ==========================

status() {
    log INFO "=== DB Tools Status ==="
    echo
    
    # Check if locked
    if [[ -f "$LOCK_FILE" ]]; then
        echo "🔒 Status: OPERATION IN PROGRESS"
        local lock_info=$(cat "$LOCK_FILE")
        echo "   $lock_info"
        
        # Check if process is still running
        if [[ "$lock_info" =~ PID:([0-9]+) ]]; then
            local pid="${BASH_REMATCH[1]}"
            if kill -0 "$pid" 2>/dev/null; then
                echo "   ✅ Process $pid is running"
            else
                echo "   ⚠️  Process $pid is NOT running (stale lock?)"
            fi
        fi
    else
        echo "✅ Status: IDLE (no operations in progress)"
    fi
    
    echo
    echo "Recent backups:"
    ls -lht "$BACKUP_DIR"/*.tar.* 2>/dev/null | head -5 || echo "  No backups found"
    
    echo
    echo "Disk space:"
    df -h "$BACKUP_DIR" | awk 'NR==1 || NR==2'
    
    echo
    echo "MySQL status:"
    if systemctl is-active --quiet mysql || systemctl is-active --quiet mariadb; then
        echo "  ✅ MySQL is running"
    else
        echo "  ❌ MySQL is NOT running"
    fi
}

# ========================== Main Dispatcher ==========================

# Load configuration
load_config

# Check core dependencies early
check_core_dependencies

# Auto-tune PARALLEL_JOBS if not set or invalid
if ! [[ "${PARALLEL_JOBS:-}" =~ ^[1-9][0-9]*$ ]]; then
  PARALLEL_JOBS="$(auto_parallel_jobs)"
  debug "Auto-tuned PARALLEL_JOBS=$PARALLEL_JOBS"
fi
# Keep XTRABACKUP_PARALLEL/XTRABACKUP_COMPRESS_THREADS in sync if they still use defaults
: "${XTRABACKUP_PARALLEL:=$PARALLEL_JOBS}"
: "${XTRABACKUP_COMPRESS_THREADS:=$PARALLEL_JOBS}"


# Create directories
mkdir -p "$BACKUP_DIR" "$MARK_DIR" 2>/dev/null || true

# Clean up orphaned partial files on startup (if enabled)
if [[ -d "$BACKUP_DIR" ]] && [[ "$SPACE_CLEANUP_PARTIAL" == "1" ]]; then
    cleanup_partial_files 2>/dev/null || true
fi

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
    status)
        status "$@"
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
    tune-parallel)
        echo "Auto-tuned PARALLEL_JOBS: $(auto_parallel_jobs)"
        ;;
    maintain)
        maintain "${1:-quick}"
        ;;
    cleanup)
        cleanup "${1:-$RETENTION_DAYS}"
        ;;
    space)
        show_space_summary
        check_space_warnings || true
        ;;
    config)
        generate_config "${1:-$CONFIG_FILE}"
        ;;
    genkey|generate-key)
        generate_encryption_key "${1:-$ENCRYPTION_KEY_FILE}"
        ;;
    version|-v|--version)
        echo "db-tools version $DB_TOOLS_VERSION"
        ;;
    help|-h|--help|"")
        usage
        ;;
    *)
        err "Unknown command: $cmd (use '$0 help' for usage)"
        ;;
esac

exit 0
