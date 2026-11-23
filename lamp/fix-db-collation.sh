#!/bin/bash

# MySQL / MariaDB utf8mb4 Collation Converter
#
# Converts database tables and textual columns to the utf8mb4 character set with
# an appropriate collation for the detected server version. Supports MySQL 5.7+,
# MySQL 8.0+ and MariaDB 10.x+.
#
# The table-level "ALTER TABLE ... CONVERT TO CHARACTER SET" is the main
# operation: MySQL rewrites every textual column internally, preserving defaults,
# ON UPDATE clauses, generated expressions and attributes. A per-column MODIFY
# pass then handles any column whose collation was pinned explicitly and so was
# left behind. Columns with generated/expression defaults that cannot be rebuilt
# safely from information_schema are reported for manual conversion rather than
# rewritten with reconstructed DDL.

set -euo pipefail

# --- Colors ---
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

# --- Configuration / state ---
MYSQL_USER=""
MYSQL_PASSWORD=""
MYSQL_HOST="localhost"
MYSQL_PORT="3306"
DATABASE_NAME=""
TARGET_COLLATION=""
DRY_RUN=false
VERBOSE=false
ASSUME_YES=false

# Temp option file holding the credentials, so the password never appears in the
# process list (ps) and special characters in it never break a command line.
MYSQL_CNF=""
# Temp file capturing stderr of the last statement.
MYSQL_ERR=""

# --- Counters ---
TABLES_CONVERTED=0
TABLES_SKIPPED=0
TABLES_WITH_ERRORS=0
COLUMNS_CONVERTED=0
COLUMNS_SKIPPED=0
COLUMNS_WITH_ERRORS=0
ERRORS=()

cleanup() {
    [[ -n "$MYSQL_CNF" && -f "$MYSQL_CNF" ]] && rm -f "$MYSQL_CNF"
    [[ -n "$MYSQL_ERR" && -f "$MYSQL_ERR" ]] && rm -f "$MYSQL_ERR"
    # Never let the trap's last command leak a non-zero status into the script's
    # exit code (it would, e.g., make --help or a clean run report failure).
    return 0
}
trap cleanup EXIT

# Increment a counter by name without tripping `set -e` on the 0 -> 1 step
# (bash's `((x++))` returns the pre-increment value, which is a non-zero exit
# status when x was 0). printf -v keeps this portable to bash 3.1+ (no nameref).
inc() {
    printf -v "$1" '%d' "$(( ${!1} + 1 ))"
}

# --- Output helpers ---
print_message() {
    local color="$1" message="$2" always_show="${3:-false}"
    if [[ "$VERBOSE" == "true" || "$always_show" == "true" ]]; then
        echo -e "${color}${message}${NC}"
    fi
}
print_error()   { echo -e "${RED}ERROR: $1${NC}" >&2; exit 1; }
print_success() { print_message "$GREEN" "✓ $1" true; }
print_info()    { print_message "$BLUE" "ℹ $1" true; }
print_warning() { print_message "$YELLOW" "⚠ $1" true; }

show_usage() {
    cat <<EOF
Usage: $0 [OPTIONS]

Options:
    -h, --help              Show this help message
    -u, --user USER         MySQL username
    -p, --password PASS     MySQL password (prompted for if omitted)
    -H, --host HOST         MySQL host (default: localhost)
    -P, --port PORT         MySQL port (default: 3306)
    -d, --database DB       Database name (prompted for if omitted)
    -n, --dry-run           Show what would be changed without making changes
    -y, --yes               Do not prompt for confirmation before converting
    -v, --verbose           Enable verbose output

Examples:
    $0 -u root -p secret
    $0 -u root -p secret -d mydatabase
    $0 -u root -p secret -d mydatabase --dry-run
EOF
}

parse_arguments() {
    while [[ $# -gt 0 ]]; do
        case "$1" in
            -h|--help)     show_usage; exit 0 ;;
            -u|--user)     MYSQL_USER="${2:-}"; shift 2 ;;
            -p|--password) MYSQL_PASSWORD="${2:-}"; shift 2 ;;
            -H|--host)     MYSQL_HOST="${2:-}"; shift 2 ;;
            -P|--port)     MYSQL_PORT="${2:-}"; shift 2 ;;
            -d|--database) DATABASE_NAME="${2:-}"; shift 2 ;;
            -n|--dry-run)  DRY_RUN=true; shift ;;
            -y|--yes)      ASSUME_YES=true; shift ;;
            -v|--verbose)  VERBOSE=true; shift ;;
            *)             print_error "Unknown option: $1" ;;
        esac
    done
}

# --- SQL quoting helpers ---

# Quote a value for a MySQL option file (handles #, quotes, backslashes).
cnf_quote() {
    local v="$1"
    v="${v//\\/\\\\}"
    if [[ "$v" == *\"* && "$v" != *\'* ]]; then
        printf "'%s'" "$v"
    else
        v="${v//\"/\\\"}"
        printf '"%s"' "$v"
    fi
}

# Escape an identifier for use inside backticks (double any backtick).
quote_ident() { printf '%s' "${1//\`/\`\`}"; }

# Escape a value as a single-quoted SQL string literal (outer quotes included).
# Double every backslash, and double every single quote (ANSI SQL quote
# doubling, valid in every sql_mode). The quote replacement below is two
# LITERAL single quotes, not a backslash escape.
quote_literal() {
    local s="$1"
    s="${s//\\/\\\\}"
    s="${s//\'/''}"
    printf "'%s'" "$s"
}

# --- Connection ---

write_mysql_cnf() {
    MYSQL_CNF=$(mktemp)
    MYSQL_ERR=$(mktemp)
    chmod 600 "$MYSQL_CNF"
    {
        printf '[client]\n'
        printf 'user=%s\n' "$(cnf_quote "$MYSQL_USER")"
        printf 'password=%s\n' "$(cnf_quote "$MYSQL_PASSWORD")"
        printf 'host=%s\n' "$(cnf_quote "$MYSQL_HOST")"
        printf 'port=%s\n' "$MYSQL_PORT"
    } >"$MYSQL_CNF"
}

# Run mysql with the credentials from the temp option file. Extra args appended.
run_mysql() { mysql --defaults-extra-file="$MYSQL_CNF" "$@"; }

prompt_for_credentials() {
    if [[ -z "$MYSQL_USER" ]]; then
        read -rp "MySQL username: " MYSQL_USER
    fi
    if [[ -z "$MYSQL_PASSWORD" ]]; then
        read -rsp "MySQL password: " MYSQL_PASSWORD
        echo
    fi
    if [[ -z "$MYSQL_USER" ]]; then
        print_error "Username is required"
    fi
}

validate_mysql_connection() {
    print_info "Testing MySQL connection..."
    if ! run_mysql -e "SELECT 1;" &>/dev/null; then
        print_error "Failed to connect to MySQL. Please check your credentials."
    fi
    print_success "MySQL connection successful"
}

# Pick a target collation for the detected server, and confirm it actually
# exists on this server before using it.
get_target_collation() {
    local version_output major minor is_mariadb=false
    version_output=$(run_mysql -s -N -e "SELECT VERSION();")
    [[ "$version_output" == *MariaDB* || "$version_output" == *mariadb* ]] && is_mariadb=true
    major=$(echo "$version_output" | grep -oE '^[0-9]+' || echo 0)
    minor=$(echo "$version_output" | sed -E 's/^[0-9]+\.([0-9]+).*/\1/' || echo 0)

    print_info "Server version detected: $version_output"

    local candidate
    if $is_mariadb; then
        candidate="utf8mb4_unicode_ci"
    elif (( major > 8 )) || (( major == 8 && minor >= 0 )); then
        candidate="utf8mb4_0900_ai_ci"
    elif (( major == 5 && minor >= 7 )) || (( major > 5 )); then
        candidate="utf8mb4_unicode_ci"
    else
        print_error "Server version $version_output is not supported (need MySQL 5.7+ or MariaDB 10+)."
    fi

    if ! collation_exists "$candidate"; then
        print_warning "Collation $candidate not available on this server; falling back to utf8mb4_general_ci."
        candidate="utf8mb4_general_ci"
        collation_exists "$candidate" || print_error "No usable utf8mb4 collation found on this server."
    fi

    TARGET_COLLATION="$candidate"
    print_info "Using collation: $TARGET_COLLATION"
}

collation_exists() {
    local coll="$1" found
    found=$(run_mysql -s -N -e "SHOW COLLATION WHERE Collation = $(quote_literal "$coll");")
    [[ -n "$found" ]]
}

# --- Database selection ---

list_databases() {
    local databases
    databases=$(run_mysql -s -N -e "SHOW DATABASES;" |
        grep -vE '^(information_schema|mysql|performance_schema|sys)$' || true)

    if [[ -z "$databases" ]]; then
        print_error "No accessible databases found"
    fi

    echo -e "${BOLD}Available databases:${NC}"
    local i=1 db_array=()
    while IFS= read -r db; do
        echo "$i) $db"
        db_array+=("$db")
        i=$((i + 1))
    done <<<"$databases"

    echo
    local selection
    read -rp "Select database number (1-$((i - 1))): " selection
    if ! [[ "$selection" =~ ^[0-9]+$ ]] || (( selection < 1 )) || (( selection > i - 1 )); then
        print_error "Invalid selection"
    fi
    DATABASE_NAME="${db_array[$((selection - 1))]}"
    print_info "Selected database: $DATABASE_NAME"
}

validate_database() {
    local db_exists
    db_exists=$(run_mysql -s -N -e \
        "SELECT SCHEMA_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = $(quote_literal "$DATABASE_NAME");")
    if [[ -z "$db_exists" ]]; then
        print_error "Database '$DATABASE_NAME' does not exist or is not accessible"
    fi
}

# --- Table / column queries ---

get_base_tables() {
    run_mysql -s -N -e \
        "SELECT TABLE_NAME, TABLE_COLLATION,
                ROUND((data_length + index_length) / 1024 / 1024, 2) AS Size_MB
         FROM information_schema.tables
         WHERE table_schema = $(quote_literal "$DATABASE_NAME")
           AND TABLE_TYPE = 'BASE TABLE'
         ORDER BY Size_MB ASC;"
}

# Columns whose collation still differs from the target after the table convert.
# GENERATION_EXPRESSION lets us detect generated columns, which must never be
# rebuilt with a plain MODIFY.
get_columns_needing_conversion() {
    local table_name="$1"
    run_mysql -s -N -e \
        "SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, COLUMN_DEFAULT, EXTRA,
                COLLATION_NAME, COLUMN_COMMENT, GENERATION_EXPRESSION
         FROM information_schema.columns
         WHERE table_schema = $(quote_literal "$DATABASE_NAME")
           AND table_name = $(quote_literal "$table_name")
           AND DATA_TYPE IN ('char','varchar','text','tinytext','mediumtext','longtext','enum','set')
           AND COLLATION_NAME IS NOT NULL
           AND COLLATION_NAME <> $(quote_literal "$TARGET_COLLATION")
         ORDER BY ORDINAL_POSITION;"
}

# --- Conversion ---

convert_table() {
    local table_name="$1" current_collation="$2" table_size="$3"
    print_message "$CYAN" "⚙ Converting table: $table_name (${table_size}MB) from ${current_collation:-?} to $TARGET_COLLATION" true

    local sql="ALTER TABLE \`$(quote_ident "$DATABASE_NAME")\`.\`$(quote_ident "$table_name")\` CONVERT TO CHARACTER SET utf8mb4 COLLATE $TARGET_COLLATION;"

    if [[ "$DRY_RUN" == "true" ]]; then
        print_message "$YELLOW" "  🔍 DRY RUN: $sql"
        return 0
    fi

    local start end
    start=$(date +%s)
    if run_mysql -e "$sql" 2>"$MYSQL_ERR"; then
        end=$(date +%s)
        print_success "Table $table_name converted in $((end - start))s"
        inc TABLES_CONVERTED
        return 0
    fi
    local details
    details=$(cat "$MYSQL_ERR" 2>/dev/null || echo "unknown error")
    local msg="Failed to convert table $table_name: $details"
    print_message "$RED" "  ❌ $msg"
    ERRORS+=("$msg")
    inc TABLES_WITH_ERRORS
    return 1
}

# Build a safe column definition, or return 1 if the column cannot be rebuilt
# from information_schema without risk (generated column, or an expression
# default we cannot quote correctly).
build_column_definition() {
    local column_type="$1" is_nullable="$2" column_default="$3" extra="$4" \
        column_comment="$5" generation_expr="$6"

    # Generated columns cannot be reconstructed safely here.
    [[ -n "$generation_expr" ]] && return 1

    local def="$column_type CHARACTER SET utf8mb4 COLLATE $TARGET_COLLATION"

    if [[ "$is_nullable" == "NO" ]]; then
        def="$def NOT NULL"
    else
        def="$def NULL"
    fi

    # EXTRA in MySQL 8 carries a DEFAULT_GENERATED marker for expression
    # defaults; strip it and detect it so the default is emitted unquoted.
    local extra_lc expr_default=false
    extra_lc=$(echo "$extra" | tr '[:upper:]' '[:lower:]')
    if [[ "$extra_lc" == *default_generated* ]]; then
        expr_default=true
    fi

    if [[ -n "$column_default" && "$column_default" != "NULL" ]]; then
        case "$column_default" in
            CURRENT_TIMESTAMP|current_timestamp*|CURRENT_TIMESTAMP*|now\(\)|NOW\(\))
                def="$def DEFAULT $column_default" ;;
            *)
                if $expr_default; then
                    # An arbitrary expression default we should not quote and
                    # cannot reliably reproduce: bail out and report instead.
                    return 1
                fi
                def="$def DEFAULT $(quote_literal "$column_default")" ;;
        esac
    fi

    # Keep auto_increment and ON UPDATE; drop the DEFAULT_GENERATED marker.
    local extra_clean
    extra_clean=$(echo "$extra" | sed -E 's/DEFAULT_GENERATED//Ig; s/^[[:space:]]+//; s/[[:space:]]+$//')
    if [[ -n "$extra_clean" ]]; then
        def="$def $extra_clean"
    fi

    if [[ -n "$column_comment" && "$column_comment" != "NULL" ]]; then
        def="$def COMMENT $(quote_literal "$column_comment")"
    fi

    printf '%s' "$def"
}

verify_column_conversion() {
    local table_name="$1" column_name="$2" coll
    coll=$(run_mysql -s -N -e \
        "SELECT COLLATION_NAME FROM information_schema.columns
         WHERE table_schema = $(quote_literal "$DATABASE_NAME")
           AND table_name = $(quote_literal "$table_name")
           AND column_name = $(quote_literal "$column_name");")
    [[ "$coll" == "$TARGET_COLLATION" ]]
}

convert_column() {
    local table_name="$1" column_name="$2" column_type="$3" is_nullable="$4" \
        column_default="$5" extra="$6" current_collation="$7" column_comment="$8" \
        generation_expr="$9"

    local def
    if ! def=$(build_column_definition "$column_type" "$is_nullable" \
        "$column_default" "$extra" "$column_comment" "$generation_expr"); then
        print_warning "  Skipping column $table_name.$column_name (generated or expression default); convert it manually."
        inc COLUMNS_SKIPPED
        return 0
    fi

    print_message "$CYAN" "  ⚙ Converting column: $column_name (${current_collation:-?} → $TARGET_COLLATION)" true

    local sql="ALTER TABLE \`$(quote_ident "$DATABASE_NAME")\`.\`$(quote_ident "$table_name")\` MODIFY COLUMN \`$(quote_ident "$column_name")\` $def;"

    if [[ "$DRY_RUN" == "true" ]]; then
        print_message "$YELLOW" "  🔍 DRY RUN: $sql"
        return 0
    fi

    [[ "$VERBOSE" == "true" ]] && print_message "$BLUE" "    SQL: $sql"

    if run_mysql -e "$sql" 2>"$MYSQL_ERR"; then
        if verify_column_conversion "$table_name" "$column_name"; then
            print_message "$GREEN" "  ✓ Column $column_name converted and verified" true
            inc COLUMNS_CONVERTED
            return 0
        fi
        local msg="Column $table_name.$column_name conversion reported success but verification failed"
        print_message "$RED" "  ❌ $msg"
        ERRORS+=("$msg")
        inc COLUMNS_WITH_ERRORS
        return 1
    fi
    local details
    details=$(cat "$MYSQL_ERR" 2>/dev/null || echo "unknown error")
    local msg="Failed to convert column $table_name.$column_name: $details"
    print_message "$RED" "  ❌ $msg"
    print_message "$RED" "    SQL was: $sql"
    ERRORS+=("$msg")
    inc COLUMNS_WITH_ERRORS
    return 1
}

show_progress() {
    local current="$1" total="$2" table_name="$3" width=40
    (( total == 0 )) && return 0
    local pct=$((current * 100 / total))
    local done=$((current * width / total))
    local bar=""
    local i
    for ((i = 0; i < done; i++)); do bar+="="; done
    for ((i = done; i < width; i++)); do bar+=" "; done
    local name="$table_name"
    (( ${#name} > 20 )) && name="${name:0:17}..."
    printf "\r[%s] %3d%% (%d/%d) %-20s" "$bar" "$pct" "$current" "$total" "$name"
}

process_tables() {
    local tables_info
    tables_info=$(get_base_tables)
    if [[ -z "$tables_info" ]]; then
        print_info "No base tables found in database $DATABASE_NAME"
        return 0
    fi

    local table_array=()
    while IFS=$'\t' read -r table_name table_collation size_mb; do
        [[ -n "$table_name" ]] && table_array+=("$table_name	$table_collation	$size_mb")
    done <<<"$tables_info"

    local total=${#table_array[@]}
    print_info "Found $total table(s) to process"
    echo

    local current=0 start
    start=$(date +%s)
    for row in "${table_array[@]}"; do
        IFS=$'\t' read -r table_name table_collation size_mb <<<"$row"
        current=$((current + 1))
        show_progress "$current" "$total" "$table_name"
        [[ "$VERBOSE" == "true" ]] && { echo; print_info "Processing $current/$total: $table_name"; }

        if [[ "$table_collation" != "$TARGET_COLLATION" ]]; then
            echo
            convert_table "$table_name" "$table_collation" "$size_mb" || true
        else
            print_message "$GREEN" "✓ Table $table_name (${size_mb}MB) already uses $TARGET_COLLATION"
            inc TABLES_SKIPPED
        fi

        # Per-column pass for any column whose collation was pinned and so was
        # not changed by the table-level convert.
        local columns_info
        columns_info=$(get_columns_needing_conversion "$table_name")
        if [[ -n "$columns_info" ]]; then
            echo
            print_message "$CYAN" "⚙ $table_name has column(s) with a pinned collation; converting individually" true
            while IFS=$'\t' read -r c_name c_type c_null c_default c_extra c_coll c_comment c_genexpr; do
                [[ -n "$c_name" ]] || continue
                convert_column "$table_name" "$c_name" "$c_type" "$c_null" \
                    "$c_default" "$c_extra" "$c_coll" "$c_comment" "$c_genexpr" || true
            done <<<"$columns_info"
        fi
    done

    echo
    local end
    end=$(date +%s)
    print_info "All tables processed in $((end - start))s"
}

display_summary() {
    echo
    echo -e "${BOLD}=======================================${NC}"
    echo -e "${BOLD}         CONVERSION SUMMARY            ${NC}"
    echo -e "${BOLD}=======================================${NC}"
    echo -e "${BOLD}Database:${NC} $DATABASE_NAME"
    echo -e "${BOLD}Target Collation:${NC} $TARGET_COLLATION"
    echo -e "${BOLD}Mode:${NC} $([[ "$DRY_RUN" == "true" ]] && echo "Dry Run" || echo "Live Run")"
    echo
    echo -e "${BOLD}Tables Converted:${NC} ${GREEN}$TABLES_CONVERTED${NC}"
    echo -e "${BOLD}Tables Skipped:${NC}   ${YELLOW}$TABLES_SKIPPED${NC}"
    echo -e "${BOLD}Tables Errored:${NC}   $([[ $TABLES_WITH_ERRORS -gt 0 ]] && echo -e "${RED}$TABLES_WITH_ERRORS${NC}" || echo 0)"
    echo
    echo -e "${BOLD}Columns Converted:${NC} ${GREEN}$COLUMNS_CONVERTED${NC}"
    echo -e "${BOLD}Columns Skipped:${NC}   ${YELLOW}$COLUMNS_SKIPPED${NC}"
    echo -e "${BOLD}Columns Errored:${NC}   $([[ $COLUMNS_WITH_ERRORS -gt 0 ]] && echo -e "${RED}$COLUMNS_WITH_ERRORS${NC}" || echo 0)"

    if (( ${#ERRORS[@]} > 0 )); then
        echo
        echo -e "${BOLD}${RED}ERRORS ENCOUNTERED:${NC}"
        local e
        for e in "${ERRORS[@]}"; do
            echo -e "${RED}- $e${NC}"
        done
    fi

    echo
    if (( ${#ERRORS[@]} == 0 )); then
        print_success "All operations completed successfully."
    else
        print_warning "Conversion completed with errors (see above)."
    fi
}

main() {
    echo -e "${BOLD}MySQL / MariaDB utf8mb4 Collation Converter${NC}"
    echo "==========================================="
    echo

    parse_arguments "$@"
    prompt_for_credentials
    write_mysql_cnf
    validate_mysql_connection
    get_target_collation

    if [[ -z "$DATABASE_NAME" ]]; then
        list_databases
    else
        validate_database
        print_info "Using database: $DATABASE_NAME"
    fi

    echo
    if [[ "$DRY_RUN" == "true" ]]; then
        print_warning "DRY RUN MODE: no changes will be made"
    else
        print_warning "LIVE MODE: changes will be applied to '$DATABASE_NAME'. Back up first."
    fi

    if [[ "$DRY_RUN" != "true" && "$ASSUME_YES" != "true" ]]; then
        echo
        local confirm
        read -rp "Continue with the conversion? (y/N): " confirm
        if [[ ! "$confirm" =~ ^[Yy]$ ]]; then
            print_info "Operation cancelled by user"
            exit 0
        fi
    fi

    echo
    print_info "Starting conversion..."
    process_tables
    display_summary

    (( ${#ERRORS[@]} > 0 )) && exit 1 || exit 0
}

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi
