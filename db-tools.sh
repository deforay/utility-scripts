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

# Auto-install behavior
AUTO_INSTALL="${AUTO_INSTALL:-0}"  # Set to 1 to enable auto-installation of tools

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
  # ratio * largest_table (rounded up)
  ratio_need_mb=$(python3 - <<PY 2>/dev/null || echo 0
r = float("${SAFE_MIN_FREE_RATIO:-2.0}")
l = int("${largest_mb:-0}")
print(int(round(r*l)))
PY
)
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

# Critical cleanup on exit/interrupt
emergency_cleanup() {
    local exit_code=$?
    local signal="${1:-EXIT}"
    
    # Only log error if this is an actual error (not normal exit)
    if [[ $exit_code -ne 0 ]]; then
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
  local elapsed=0
  while [[ -f "$LOCK_FILE" ]]; do
    if (( elapsed >= timeout )); then
      local lock_info=$(cat "$LOCK_FILE" 2>/dev/null || echo "unknown")
      
      # Check if the process is still running
      if [[ "$lock_info" =~ PID:([0-9]+) ]]; then
        local lock_pid="${BASH_REMATCH[1]}"
        if ! kill -0 "$lock_pid" 2>/dev/null; then
          warn "Stale lock file found (PID $lock_pid not running), removing..."
          rm -f "$LOCK_FILE"
          break
        fi
      fi
      
      err "Lock timeout: Another instance running for ${elapsed}s. Lock info: $lock_info"
    fi
    
    debug "Waiting for lock... (${elapsed}s) - Held by: $(cat "$LOCK_FILE" 2>/dev/null || echo 'unknown')"
    sleep 5
    ((elapsed+=5))
  done

  echo "PID:$$ OPERATION:$operation USER:$(whoami) STARTED:$(date '+%Y-%m-%d %H:%M:%S')" > "$LOCK_FILE"
  stack_trap 'release_lock' EXIT
  debug "Lock acquired for '$operation' (PID: $$)"
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
            if [[ "${AUTO_INSTALL:-0}" != "1" ]]; then
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
        curl -s -X POST "$NOTIFY_WEBHOOK" \
            -H "Content-Type: application/json" \
            -d "{\"subject\":\"$subject\",\"message\":\"$message\",\"level\":\"$level\",\"host\":\"$(hostname)\"}" \
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

# ========================== Progress Functions ==========================

# ========================== Initialization ==========================

init() {
    need_tooling  # ← This will auto-install XtraBackup!
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
    
    ensure_compression_tools
    check_key_perms
    
    mkdir -p "$BACKUP_DIR" "$MARK_DIR" "$LOG_DIR" || err "Failed to create necessary directories"
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
    acquire_lock 300 "backup-$backup_type"  # ← Updated with operation name
    
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
    OPERATION_IN_PROGRESS="xtrabackup-full"
    
    local ts="$(date +'%Y-%m-%d-%H-%M-%S')"
    local backup_dir="$BACKUP_DIR/xtra-full-$ts"
    
    log INFO "Starting XtraBackup full backup..."
    
    # Check disk space BEFORE starting backup
    local estimated_size=$(estimate_backup_size)
    check_backup_space "$BACKUP_DIR" "$estimated_size" 4
    
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
    mysql_host=$(mysql_config_editor print --login-path="$LOGIN_PATH" 2>/dev/null | grep host | cut -d= -f2 | tr -d ' ' | tr -d '"')
    mysql_port=$(mysql_config_editor print --login-path="$LOGIN_PATH" 2>/dev/null | grep port | cut -d= -f2 | tr -d ' ' | tr -d '"')
    mysql_user=$(mysql_config_editor print --login-path="$LOGIN_PATH" 2>/dev/null | grep user | cut -d= -f2 | tr -d ' ' | tr -d '"')
    
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
            echo "xtrabackup_version=$($XTRABACKUP --version 2>&1 | head -1)"
            echo "server_version=$("$MYSQL" --login-path="$LOGIN_PATH" -N -e 'SELECT VERSION();')"
            echo "compression_algo=$COMPRESS_ALGO"
            echo "compression_level=$COMPRESS_LEVEL"
            echo "parallel_jobs=$PARALLEL_JOBS"
            echo "backup_method=$BACKUP_METHOD"
            if [[ "$ENCRYPT_BACKUPS" == "1" ]]; then
                echo "encryption_cipher=aes-256-gcm"
            fi         
            
            # Get GTID info if enabled
            local gtid_mode
            gtid_mode=$("$MYSQL" --login-path="$LOGIN_PATH" -N -e "SHOW VARIABLES LIKE 'gtid_mode';" 2>/dev/null | awk '{print $2}' || echo "OFF")
            echo "gtid_mode=$gtid_mode"
            
            if [[ "$gtid_mode" == "ON" ]]; then
                local gtid_executed
                gtid_executed=$("$MYSQL" --login-path="$LOGIN_PATH" -N -e "SELECT @@GLOBAL.gtid_executed;" 2>/dev/null || echo "")
                echo "gtid_executed=$gtid_executed"
            fi
            
            # Extract LSN from xtrabackup_checkpoints
            if [[ -f "$backup_dir/xtrabackup_checkpoints" ]]; then
                grep "^to_lsn" "$backup_dir/xtrabackup_checkpoints"
            fi
            
            # Get binlog position
            if [[ -f "$backup_dir/xtrabackup_binlog_info" ]]; then
                local binlog_info
                binlog_info=$(cat "$backup_dir/xtrabackup_binlog_info")
                echo "binlog_file=$(echo "$binlog_info" | awk '{print $1}')" 
                echo "binlog_pos=$(echo "$binlog_info" | awk '{print $2}')"
            fi
        } > "$BACKUP_DIR/backup-$ts.meta"
        
        # Compress/package backup with .partial suffix
        log INFO "Compressing backup..."
        if [[ "$ENCRYPT_BACKUPS" == "1" ]]; then
            tar -cf - -C "$BACKUP_DIR" "xtra-full-$ts" | \
                encrypt_if_enabled > "$BACKUP_DIR/xtra-full-$ts.tar.enc.partial"
            
            mv "$BACKUP_DIR/xtra-full-$ts.tar.enc.partial" "$BACKUP_DIR/xtra-full-$ts.tar.enc"
            rm -rf "$backup_dir"
            backup_dir="$BACKUP_DIR/xtra-full-$ts.tar.enc"
        else
            # Don't double-compress! XtraBackup already compressed
            tar -cf "$BACKUP_DIR/xtra-full-$ts.tar.partial" -C "$BACKUP_DIR" "xtra-full-$ts"
            
            mv "$BACKUP_DIR/xtra-full-$ts.tar.partial" "$BACKUP_DIR/xtra-full-$ts.tar"
            rm -rf "$backup_dir"
            backup_dir="$BACKUP_DIR/xtra-full-$ts.tar"
        fi
        
        # Create checksum
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
    
    # Find base backup (physical full)
    local base_backup
    base_backup=$(find "$BACKUP_DIR" -maxdepth 1 \( -name "xtra-full-*.tar.gz" -o -name "xtra-full-*.tar.enc" -o -name "xtra-full-*.tar" \) 2>/dev/null | sort -r | head -1)

    if [[ -z "$base_backup" ]]; then
        warn "No full XtraBackup found, creating full backup instead"
        backup_xtra_full
        return
    fi
    
    log INFO "Starting XtraBackup incremental backup..."
    log INFO "Base backup: $(basename "$base_backup")"
    
    # Extract base backup to temp location
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
    
    # Build incremental command
    local xtra_cmd=("$XTRABACKUP" --backup)
    xtra_cmd+=(--target-dir="$backup_dir")
    xtra_cmd+=(--incremental-basedir="$base_dir")
    xtra_cmd+=(--parallel="$XTRABACKUP_PARALLEL")
    
    # Add compression
    if [[ "$XTRABACKUP_COMPRESS" == "1" ]]; then
        case "$COMPRESS_ALGO" in
            zstd)
                if have zstd; then
                    xtra_cmd+=(--compress=zstd)
                    xtra_cmd+=(--compress-threads="$XTRABACKUP_COMPRESS_THREADS")
                fi
                ;;
            *)
                if have qpress; then
                    xtra_cmd+=(--compress)
                    xtra_cmd+=(--compress-threads="$XTRABACKUP_COMPRESS_THREADS")
                fi
                ;;
        esac
    fi
    
    # Credentials from login-path
    local mysql_user mysql_host mysql_port
    mysql_host=$(mysql_config_editor print --login-path="$LOGIN_PATH" 2>/dev/null | grep host | cut -d= -f2 | tr -d ' ' | tr -d '"')
    mysql_port=$(mysql_config_editor print --login-path="$LOGIN_PATH" 2>/dev/null | grep port | cut -d= -f2 | tr -d ' ' | tr -d '"')
    mysql_user=$(mysql_config_editor print --login-path="$LOGIN_PATH" 2>/dev/null | grep user | cut -d= -f2 | tr -d ' ' | tr -d '"')
    
    [[ -n "$mysql_host" ]] && xtra_cmd+=(--host="$mysql_host")
    [[ -n "$mysql_port" ]] && xtra_cmd+=(--port="$mysql_port")
    [[ -n "$mysql_user" ]] && xtra_cmd+=(--user="$mysql_user")
    
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
            if [[ "$ENCRYPT_BACKUPS" == "1" ]]; then
                echo "encryption_cipher=aes-256-gcm"
            fi
            if [[ -f "$backup_dir/xtrabackup_checkpoints" ]]; then
                grep "^from_lsn" "$backup_dir/xtrabackup_checkpoints"
                grep "^to_lsn" "$backup_dir/xtrabackup_checkpoints"
            fi
        } > "$BACKUP_DIR/backup-$ts.meta"
        
        log INFO "Compressing incremental backup..."
        if [[ "$ENCRYPT_BACKUPS" == "1" ]]; then
            tar -cf - -C "$BACKUP_DIR" "xtra-incr-$ts" | \
                encrypt_if_enabled > "$BACKUP_DIR/xtra-incr-$ts.tar.enc.partial"
            mv "$BACKUP_DIR/xtra-incr-$ts.tar.enc.partial" "$BACKUP_DIR/xtra-incr-$ts.tar.enc"
            rm -rf "$backup_dir"
        else
            tar -cf "$BACKUP_DIR/xtra-incr-$ts.tar.partial" -C "$BACKUP_DIR" "xtra-incr-$ts"
            mv "$BACKUP_DIR/xtra-incr-$ts.tar.partial" "$BACKUP_DIR/xtra-incr-$ts.tar"
            rm -rf "$backup_dir"
        fi
        
        local packed="$BACKUP_DIR/xtra-incr-$ts.tar"
        [[ "$ENCRYPT_BACKUPS" == "1" ]] && packed="${packed}.enc"
        create_checksum "$packed"
        
        log INFO "✅ XtraBackup incremental backup complete: $(basename "$packed")"
        add_summary "XtraBackup incremental backup: $(basename "$packed")"
        notify "Incremental backup completed" "XtraBackup incremental backup completed" "info"
        
        OPERATION_IN_PROGRESS=""
        return 0
    else
        OPERATION_IN_PROGRESS=""
        err "XtraBackup incremental backup failed. Check $backup_dir/xtrabackup.log"
    fi
}


restore_xtra() {
    OPERATION_IN_PROGRESS="xtrabackup-restore"
    
    local backup_file="$1"
    local target_db="${2:-}"
    local until_time="${3:-}"
    
    log INFO "Restoring from XtraBackup: $(basename "$backup_file")"
    
    check_restore_space "$backup_file" 3
    verify_checksum "$backup_file" || warn "Checksum verification failed"
    
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
    [[ -z "$backup_dir" ]] && err "Cannot find extracted backup"
    
    if [[ -f "$backup_dir/xtrabackup_checkpoints" ]]; then
        if grep -q "compressed = 1" "$backup_dir/xtrabackup_checkpoints" 2>/dev/null; then
            log INFO "Decompressing backup..."
            "$XTRABACKUP" --decompress --target-dir="$backup_dir" \
                --parallel="$XTRABACKUP_PARALLEL" || err "Decompression failed"
            find "$backup_dir" -name "*.qp" -o -name "*.zst" -delete
        fi
    fi
    
    local base_ts
    base_ts=$(basename "$backup_file" | grep -oP '\d{4}-\d{2}-\d{2}-\d{2}-\d{2}-\d{2}')
    
    if [[ -z "$base_ts" ]]; then
        warn "Cannot determine timestamp from backup filename, skipping incrementals"
    else
        log INFO "Base backup timestamp: $base_ts"
        local incrementals=()
        while IFS= read -r incr; do
            local incr_ts
            incr_ts=$(basename "$incr" | grep -oP '\d{4}-\d{2}-\d{2}-\d{2}-\d{2}-\d{2}')
            if [[ "$incr_ts" > "$base_ts" ]]; then
                incrementals+=("$incr")
            else
                debug "Skipping old incremental: $(basename "$incr") (before $base_ts)"
            fi
        done < <(find "$BACKUP_DIR" -name "xtra-incr-*.tar.*" 2>/dev/null | sort)
        
        if [[ ${#incrementals[@]} -gt 0 ]]; then
            log INFO "Found ${#incrementals[@]} incremental backup(s) to apply (created after $base_ts)"
            
            log INFO "Preparing base backup..."
            "$XTRABACKUP" --prepare --apply-log-only \
                --target-dir="$backup_dir" \
                --use-memory="$XTRABACKUP_MEMORY" || err "Base prepare failed"
            
            for incr in "${incrementals[@]}"; do
                log INFO "Applying incremental: $(basename "$incr")"
                local temp_incr="$temp_restore/incr_$$"
                mkdir -p "$temp_incr"
                
                if [[ "$incr" =~ \.enc$ ]]; then
                    decrypt_if_encrypted "$incr" < "$incr" | tar -xf - -C "$temp_incr"
                else
                    tar -xf "$incr" -C "$temp_incr"
                fi
                
                local incr_dir
                incr_dir=$(find "$temp_incr" -maxdepth 1 -type d -name "xtra-incr-*" | head -1)
                
                if [[ -f "$incr_dir/xtrabackup_checkpoints" ]]; then
                    if grep -q "compressed = 1" "$incr_dir/xtrabackup_checkpoints" 2>/dev/null; then
                        "$XTRABACKUP" --decompress --target-dir="$incr_dir" \
                            --parallel="$XTRABACKUP_PARALLEL"
                        find "$incr_dir" -name "*.qp" -o -name "*.zst" -delete
                    fi
                fi
                
                "$XTRABACKUP" --prepare --apply-log-only \
                    --target-dir="$backup_dir" \
                    --incremental-dir="$incr_dir" \
                    --use-memory="$XTRABACKUP_MEMORY" || err "Incremental apply failed"
                
                rm -rf "$temp_incr"
            done
        else
            log INFO "No incremental backups found to apply"
        fi
    fi
    
    log INFO "Final prepare..."
    "$XTRABACKUP" --prepare \
        --target-dir="$backup_dir" \
        --use-memory="$XTRABACKUP_MEMORY" || err "Final prepare failed"
    
    log WARN "=== CRITICAL: Stopping MySQL for restore ==="
    log WARN "Do NOT interrupt this process!"
    MYSQL_WAS_STOPPED=1
    
    if ! stop_mysql; then
        err "Cannot stop MySQL/MariaDB"
    fi
    
    local datadir="/var/lib/mysql"
    local backup_old="$datadir.backup.$(date +%s)"
    DATADIR_BACKUP_PATH="$backup_old"
    
    log INFO "Backing up current datadir to $backup_old"
    mv "$datadir" "$backup_old" || err "Cannot backup current datadir"
    mkdir -p "$datadir"
    
    log INFO "Copying backup to datadir... (this may take several minutes)"
    if ! "$XTRABACKUP" --copy-back --target-dir="$backup_dir"; then
        log ERROR "Copy-back failed!"
        log ERROR "Attempting to restore original datadir..."
        rm -rf "$datadir"
        mv "$backup_old" "$datadir"
        DATADIR_BACKUP_PATH=""
        MYSQL_WAS_STOPPED=0
        err "Restore failed - original datadir restored"
    fi
    
    if chown -R mysql:mysql "$datadir" 2>/dev/null; then
        debug "Set ownership to mysql:mysql"
    elif chown -R mariadb:mariadb "$datadir" 2>/dev/null; then
        debug "Set ownership to mariadb:mariadb"
    else
        warn "Could not set standard ownership (mysql or mariadb user not found)"
    fi
    
    if command -v restorecon >/dev/null 2>&1; then
        log INFO "Restoring SELinux contexts..."
        restorecon -Rv "$datadir" 2>&1 | head -20 || true
    fi
    
    log INFO "Starting MySQL..."
    if start_mysql; then
        log INFO "✅ XtraBackup restore complete"
        MYSQL_WAS_STOPPED=0
        DATADIR_BACKUP_PATH=""
        
        if [[ -n "$until_time" ]]; then
            if [[ -f "$backup_dir/xtrabackup_binlog_info" ]]; then
                local binlog_file binlog_pos
                binlog_file=$(awk '{print $1}' "$backup_dir/xtrabackup_binlog_info")
                binlog_pos=$(awk '{print $2}' "$backup_dir/xtrabackup_binlog_info")
                
                log INFO "Applying binlogs from $binlog_file:$binlog_pos to $until_time"
                run_pitr_from_position "$binlog_file" "$binlog_pos" "$until_time"
            fi
        fi
        
        log INFO "Restore successful. Old datadir backup kept at: $backup_old"
        log INFO "You can remove it manually after verifying the restore: rm -rf $backup_old"
        
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
        
        # Count results from status files more reliably
        completed=0
        errors=0
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
                if decrypt_if_encrypted "$f" < "$f" tar -tf - >/dev/null 2>&1; then
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

    acquire_lock 300 "restore"
    
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
        show_progress "$idx" "${#tables[@]}" "ANALYZE"
        "$MYSQL" --login-path="$LOGIN_PATH" -e "ANALYZE TABLE ${t};" >/dev/null 2>&1 || warn "ANALYZE failed: $t"
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
                show_progress "$idx" "${#tables[@]}" "OPTIMIZE"
                # Check per-table size; skip if free space < ratio * table size
                local tmb
                tmb=$("$MYSQL" --login-path="$LOGIN_PATH" -N -e "
                  SELECT COALESCE(ROUND((data_length+index_length)/1024/1024),0)
                  FROM information_schema.TABLES
                  WHERE CONCAT(TABLE_SCHEMA,'.',TABLE_NAME)='${t}';" 2>/dev/null | awk '{print int($1)}')
                local need_mb
                need_mb=$(python3 - <<PY 2>/dev/null || echo 0
r = float("${SAFE_MIN_FREE_RATIO:-2.0}")
print(int(round(r*${tmb:-0})))
PY
)
                # If per-table check fails, skip that table
                if (( free_mb > need_mb )); then
                    "$MYSQL" --login-path="$LOGIN_PATH" -e "OPTIMIZE TABLE ${t};" >/dev/null 2>&1 || warn "OPTIMIZE failed: $t"
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