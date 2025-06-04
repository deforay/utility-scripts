#!/bin/bash

# MySQL UTF8MB4 Collation Converter
# Converts database tables and columns to utf8mb4 character set with appropriate collation
# Supports MySQL 5.7+ and 8.0+

set -euo pipefail

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m' # No Color

# Global variables
MYSQL_USER=""
MYSQL_PASSWORD=""
MYSQL_HOST="localhost"
MYSQL_PORT="3306"
DATABASE_NAME=""
TARGET_COLLATION=""
DRY_RUN=false
VERBOSE=false

# Counters for summary
TABLES_CONVERTED=0
TABLES_SKIPPED=0
TABLES_WITH_ERRORS=0
COLUMNS_CONVERTED=0
COLUMNS_WITH_ERRORS=0
ERRORS=()

# Function to print colored output
print_message() {
    local color=$1
    local message=$2
    local always_show=${3:-false}

    if [[ "$VERBOSE" == "true" || "$always_show" == "true" ]]; then
        echo -e "${color}${message}${NC}"
    fi
}

# Function to print error and exit
print_error() {
    echo -e "${RED}ERROR: $1${NC}" >&2
    exit 1
}

# Function to print success message
print_success() {
    print_message "$GREEN" "✓ $1" true
}

# Function to print info message
print_info() {
    print_message "$BLUE" "ℹ $1" true
}

# Function to print warning message
print_warning() {
    print_message "$YELLOW" "⚠ $1" true
}

# Function to show usage
show_usage() {
    cat << EOF
Usage: $0 [OPTIONS]

Options:
    -h, --help              Show this help message
    -u, --user USER         MySQL username
    -p, --password PASS     MySQL password
    -H, --host HOST         MySQL host (default: localhost)
    -P, --port PORT         MySQL port (default: 3306)
    -d, --database DB       Database name (optional)
    -n, --dry-run           Show what would be changed without making changes
    -v, --verbose           Enable verbose output

Examples:
    $0 -u root -p mypassword
    $0 -u root -p mypassword -d mydatabase
    $0 -u root -p mypassword -d mydatabase --dry-run
    $0 -u root -p mypassword --verbose

EOF
}

# Function to parse command line arguments
parse_arguments() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            -h|--help)
                show_usage
                exit 0
                ;;
            -u|--user)
                MYSQL_USER="$2"
                shift 2
                ;;
            -p|--password)
                MYSQL_PASSWORD="$2"
                shift 2
                ;;
            -H|--host)
                MYSQL_HOST="$2"
                shift 2
                ;;
            -P|--port)
                MYSQL_PORT="$2"
                shift 2
                ;;
            -d|--database)
                DATABASE_NAME="$2"
                shift 2
                ;;
            -n|--dry-run)
                DRY_RUN=true
                shift
                ;;
            -v|--verbose)
                VERBOSE=true
                shift
                ;;
            *)
                print_error "Unknown option: $1"
                ;;
        esac
    done
}

# Function to validate MySQL connection
validate_mysql_connection() {
    print_info "Testing MySQL connection..."

    if ! mysql -h"$MYSQL_HOST" -P"$MYSQL_PORT" -u"$MYSQL_USER" -p"$MYSQL_PASSWORD" -e "SELECT 1;" &>/dev/null; then
        print_error "Failed to connect to MySQL. Please check your credentials."
    fi

    print_success "MySQL connection successful"
}

# Function to get MySQL version
get_mysql_version() {
    local version_output
    version_output=$(mysql -h"$MYSQL_HOST" -P"$MYSQL_PORT" -u"$MYSQL_USER" -p"$MYSQL_PASSWORD" -e "SELECT VERSION();" -s -N)

    # Extract major and minor version numbers
    local major_version=$(echo "$version_output" | cut -d'.' -f1)
    local minor_version=$(echo "$version_output" | cut -d'.' -f2)

    print_info "MySQL version detected: $version_output"

    # Determine target collation based on version
    if [[ $major_version -gt 8 ]] || [[ $major_version -eq 8 && $minor_version -ge 0 ]]; then
        TARGET_COLLATION="utf8mb4_0900_ai_ci"
        print_info "Using MySQL 8.0+ collation: $TARGET_COLLATION"
    elif [[ $major_version -ge 5 && $minor_version -ge 7 ]]; then
        TARGET_COLLATION="utf8mb4_unicode_ci"
        print_info "Using MySQL 5.7+ collation: $TARGET_COLLATION"
    else
        print_error "MySQL version $version_output is not supported. Minimum version required: 5.7"
    fi
}

# Function to prompt for credentials if not provided
prompt_for_credentials() {
    if [[ -z "$MYSQL_USER" ]]; then
        read -p "MySQL username: " MYSQL_USER
    fi

    if [[ -z "$MYSQL_PASSWORD" ]]; then
        read -s -p "MySQL password: " MYSQL_PASSWORD
        echo
    fi

    if [[ -z "$MYSQL_USER" || -z "$MYSQL_PASSWORD" ]]; then
        print_error "Username and password are required"
    fi
}

# Function to list available databases
list_databases() {
    local databases
    databases=$(mysql -h"$MYSQL_HOST" -P"$MYSQL_PORT" -u"$MYSQL_USER" -p"$MYSQL_PASSWORD" \
        -e "SHOW DATABASES;" -s -N | grep -vE '^(information_schema|mysql|performance_schema|sys)$')

    if [[ -z "$databases" ]]; then
        print_error "No accessible databases found"
    fi

    echo -e "${BOLD}Available databases:${NC}"
    local i=1
    local db_array=()

    while IFS= read -r db; do
        echo "$i) $db"
        db_array+=("$db")
        ((i++))
    done <<< "$databases"

    echo
    read -p "Select database number (1-$((i-1))): " selection

    if ! [[ "$selection" =~ ^[0-9]+$ ]] || [[ "$selection" -lt 1 ]] || [[ "$selection" -gt $((i-1)) ]]; then
        print_error "Invalid selection"
    fi

    DATABASE_NAME="${db_array[$((selection-1))]}"
    print_info "Selected database: $DATABASE_NAME"
}

# Function to check if database exists
validate_database() {
    local db_exists
    db_exists=$(mysql -h"$MYSQL_HOST" -P"$MYSQL_PORT" -u"$MYSQL_USER" -p"$MYSQL_PASSWORD" \
        -e "SELECT SCHEMA_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME='$DATABASE_NAME';" -s -N)

    if [[ -z "$db_exists" ]]; then
        print_error "Database '$DATABASE_NAME' does not exist or is not accessible"
    fi
}

# Function to get tables that need conversion
get_tables_needing_conversion() {
    mysql -h"$MYSQL_HOST" -P"$MYSQL_PORT" -u"$MYSQL_USER" -p"$MYSQL_PASSWORD" \
        -e "SELECT TABLE_NAME, TABLE_COLLATION,
               ROUND((data_length + index_length) / 1024 / 1024, 2) AS 'Size_MB'
            FROM information_schema.tables
            WHERE table_schema = '$DATABASE_NAME'
            AND TABLE_TYPE = 'BASE TABLE'
            ORDER BY Size_MB ASC;" -s -N
}

# Function to check if table needs conversion
table_needs_conversion() {
    local table_name=$1
    local current_collation

    current_collation=$(mysql -h"$MYSQL_HOST" -P"$MYSQL_PORT" -u"$MYSQL_USER" -p"$MYSQL_PASSWORD" \
        -e "SELECT TABLE_COLLATION FROM information_schema.tables
            WHERE table_schema = '$DATABASE_NAME' AND table_name = '$table_name';" -s -N)

    [[ "$current_collation" != "$TARGET_COLLATION" ]]
}

# Function to get columns that need conversion with complete definition
get_columns_needing_conversion() {
    local table_name=$1

    mysql -h"$MYSQL_HOST" -P"$MYSQL_PORT" -u"$MYSQL_USER" -p"$MYSQL_PASSWORD" \
        -e "SELECT
                COLUMN_NAME,
                COLUMN_TYPE,
                IS_NULLABLE,
                COLUMN_DEFAULT,
                EXTRA,
                COLLATION_NAME,
                COLUMN_COMMENT
            FROM information_schema.columns
            WHERE table_schema = '$DATABASE_NAME'
            AND table_name = '$table_name'
            AND DATA_TYPE IN ('char', 'varchar', 'text', 'tinytext', 'mediumtext', 'longtext', 'enum', 'set')
            AND COLLATION_NAME IS NOT NULL
            AND COLLATION_NAME != '$TARGET_COLLATION'
            ORDER BY ORDINAL_POSITION;" -s -N
}

# Function to convert table
convert_table() {
    local table_name=$1
    local current_collation=$2
    local table_size=$3

    print_message "$CYAN" "⚙ Converting table: $table_name (${table_size}MB) from $current_collation to $TARGET_COLLATION"

    if [[ "$DRY_RUN" == "true" ]]; then
        print_message "$YELLOW" "🔍 DRY RUN: Would convert table $table_name to $TARGET_COLLATION"
        return 0
    fi

    local start_time=$(date +%s)

    if mysql -h"$MYSQL_HOST" -P"$MYSQL_PORT" -u"$MYSQL_USER" -p"$MYSQL_PASSWORD" \
        -e "ALTER TABLE \`$DATABASE_NAME\`.\`$table_name\` CONVERT TO CHARACTER SET utf8mb4 COLLATE $TARGET_COLLATION;" 2>/dev/null; then

        local end_time=$(date +%s)
        local duration=$((end_time - start_time))
        print_success "Table $table_name converted successfully in ${duration}s"
        ((TABLES_CONVERTED++))
        return 0
    else
        local error_msg="Failed to convert table $table_name"
        print_message "$RED" "❌ $error_msg"
        ERRORS+=("$error_msg")
        ((TABLES_WITH_ERRORS++))
        return 1
    fi
}

# Function to get indexes that might be affected by column changes
get_affected_indexes() {
    local table_name=$1
    local column_name=$2

    mysql -h"$MYSQL_HOST" -P"$MYSQL_PORT" -u"$MYSQL_USER" -p"$MYSQL_PASSWORD" \
        -e "SELECT DISTINCT INDEX_NAME, NON_UNIQUE, INDEX_TYPE
            FROM information_schema.statistics
            WHERE table_schema = '$DATABASE_NAME'
            AND table_name = '$table_name'
            AND column_name = '$column_name'
            AND INDEX_NAME != 'PRIMARY';" -s -N
}

# Function to show warning about potential index issues
check_index_warnings() {
    local table_name=$1
    local column_name=$2

    local indexes_info
    indexes_info=$(get_affected_indexes "$table_name" "$column_name")

    if [[ -n "$indexes_info" ]]; then
        print_message "$YELLOW" "    ⚠ Column $column_name has indexes that may be affected:"
        while IFS=
build_column_definition() {
    local column_type=$1
    local is_nullable=$2
    local column_default=$3
    local extra=$4
    local column_comment=$5

    local definition="$column_type CHARACTER SET utf8mb4 COLLATE $TARGET_COLLATION"

    # Handle NULL/NOT NULL
    if [[ "$is_nullable" == "NO" ]]; then
        definition="$definition NOT NULL"
    else
        definition="$definition NULL"
    fi

    # Handle DEFAULT values with proper escaping and special cases
    if [[ "$column_default" != "NULL" && -n "$column_default" ]]; then
        case "$column_default" in
            "CURRENT_TIMESTAMP"|"current_timestamp()"|"now()"|"CURRENT_TIMESTAMP()"|"NULL")
                definition="$definition DEFAULT $column_default"
                ;;
            *)
                # Escape single quotes and wrap in quotes for string defaults
                local escaped_default=$(printf '%s\n' "$column_default" | sed "s/'/''/g")
                definition="$definition DEFAULT '$escaped_default'"
                ;;
        esac
    fi

    # Handle EXTRA attributes (AUTO_INCREMENT, ON UPDATE, etc.)
    if [[ -n "$extra" && "$extra" != "NULL" ]]; then
        definition="$definition $extra"
    fi

    # Handle COMMENT
    if [[ -n "$column_comment" && "$column_comment" != "NULL" ]]; then
        local escaped_comment=$(printf '%s\n' "$column_comment" | sed "s/'/''/g")
        definition="$definition COMMENT '$escaped_comment'"
    fi

    echo "$definition"
}

# Function to convert column
convert_column() {
    local table_name=$1
    local column_name=$2
    local column_type=$3
    local is_nullable=$4
    local column_default=$5
    local extra=$6
    local current_collation=$7
    local column_comment=$8

    print_message "$CYAN" "  ⚙ Converting column: $column_name ($current_collation → $TARGET_COLLATION)" true

    # Check for indexes that might be affected
    if [[ "$VERBOSE" == "true" ]]; then
        check_index_warnings "$table_name" "$column_name"
    fi

    if [[ "$DRY_RUN" == "true" ]]; then
        local column_def=$(build_column_definition "$column_type" "$is_nullable" "$column_default" "$extra" "$column_comment")
        print_message "$YELLOW" "  🔍 DRY RUN: Would execute: ALTER TABLE \`$DATABASE_NAME\`.\`$table_name\` MODIFY COLUMN \`$column_name\` $column_def"
        return 0
    fi

    # Get the complete column definition preserving all properties
    local column_definition=$(build_column_definition "$column_type" "$is_nullable" "$column_default" "$extra" "$column_comment")

    local alter_sql="ALTER TABLE \`$DATABASE_NAME\`.\`$table_name\` MODIFY COLUMN \`$column_name\` $column_definition;"

    if [[ "$VERBOSE" == "true" ]]; then
        print_message "$BLUE" "    SQL: $alter_sql"
    fi

    local start_time=$(date +%s)

    if mysql -h"$MYSQL_HOST" -P"$MYSQL_PORT" -u"$MYSQL_USER" -p"$MYSQL_PASSWORD" \
        -e "$alter_sql" 2>/tmp/mysql_column_error.log; then

        # Verify the conversion was successful
        if verify_column_conversion "$table_name" "$column_name"; then
            local end_time=$(date +%s)
            local duration=$((end_time - start_time))
            print_message "$GREEN" "  ✓ Column $column_name converted and verified in ${duration}s"
            ((COLUMNS_CONVERTED++))
            return 0
        else
            local error_msg="Column $table_name.$column_name conversion appeared to succeed but verification failed"
            print_message "$RED" "  ❌ $error_msg"
            ERRORS+=("$error_msg")
            ((COLUMNS_WITH_ERRORS++))
            return 1
        fi
    else
        local error_details=$(cat /tmp/mysql_column_error.log 2>/dev/null || echo "Unknown error")
        local error_msg="Failed to convert column $table_name.$column_name: $error_details"
        print_message "$RED" "  ❌ $error_msg"
        print_message "$RED" "    SQL was: $alter_sql"
        ERRORS+=("$error_msg")
        ((COLUMNS_WITH_ERRORS++))
        return 1
    fi
}

# Function to show progress bar
show_progress() {
    local current=$1
    local total=$2
    local table_name=$3
    local width=50

    local percentage=$((current * 100 / total))
    local completed=$((current * width / total))
    local remaining=$((width - completed))

    local bar=""
    for ((i=0; i<completed; i++)); do bar+="="; done
    for ((i=0; i<remaining; i++)); do bar+=" "; done

    # Truncate table name if too long
    local display_name="$table_name"
    if [[ ${#table_name} -gt 20 ]]; then
        display_name="${table_name:0:17}..."
    fi

    printf "\r[%s] %3d%% (%d/%d) - %-20s" "$bar" "$percentage" "$current" "$total" "$display_name"
}

# Function to process all tables
process_tables() {
    local tables_info
    tables_info=$(get_tables_needing_conversion)

    if [[ -z "$tables_info" ]]; then
        print_info "No tables found in database $DATABASE_NAME"
        return 0
    fi

    local total_tables=0
    local table_array=()

    # Count total tables and build array
    while IFS=$'\t' read -r table_name table_collation size_mb; do
        table_array+=("$table_name|$table_collation|$size_mb")
        ((total_tables++))
    done <<< "$tables_info"

    print_info "Found $total_tables tables to process"
    echo

    local current_table=0
    local script_start_time=$(date +%s)

    for table_info in "${table_array[@]}"; do
        IFS='|' read -r table_name table_collation size_mb <<< "$table_info"
        ((current_table++))

        # Show progress
        show_progress "$current_table" "$total_tables" "$table_name"

        if [[ "$VERBOSE" == "true" ]]; then
            echo # New line for verbose output
            print_info "Processing table $current_table of $total_tables: $table_name"
        fi

        # Check if table needs conversion
        if table_needs_conversion "$table_name"; then
            convert_table "$table_name" "$table_collation" "$size_mb"
        else
            print_message "$GREEN" "✓ Table $table_name (${size_mb}MB) already uses $TARGET_COLLATION - skipping"
            ((TABLES_SKIPPED++))
        fi

        # Process columns
        local columns_info
        columns_info=$(get_columns_needing_conversion "$table_name")

        if [[ -n "$columns_info" ]]; then
            local column_count=0
            while IFS=
    done

    echo # New line after progress bar

    local script_end_time=$(date +%s)
    local total_duration=$((script_end_time - script_start_time))

    print_info "All tables processed in ${total_duration}s"
}

# Function to display summary
display_summary() {
    echo
    echo -e "${BOLD}=======================================${NC}"
    echo -e "${BOLD}         CONVERSION SUMMARY           ${NC}"
    echo -e "${BOLD}=======================================${NC}"

    echo -e "${BOLD}Database:${NC} $DATABASE_NAME"
    echo -e "${BOLD}Target Collation:${NC} $TARGET_COLLATION"
    echo -e "${BOLD}Mode:${NC} $(if [[ "$DRY_RUN" == "true" ]]; then echo "Dry Run"; else echo "Live Run"; fi)"
    echo

    echo -e "${BOLD}Tables Converted:${NC} ${GREEN}$TABLES_CONVERTED${NC}"
    echo -e "${BOLD}Tables Skipped:${NC} ${YELLOW}$TABLES_SKIPPED${NC}"
    echo -e "${BOLD}Tables With Errors:${NC} $(if [[ $TABLES_WITH_ERRORS -gt 0 ]]; then echo -e "${RED}$TABLES_WITH_ERRORS${NC}"; else echo "0"; fi)"
    echo

    echo -e "${BOLD}Columns Converted:${NC} ${GREEN}$COLUMNS_CONVERTED${NC}"
    echo -e "${BOLD}Columns With Errors:${NC} $(if [[ $COLUMNS_WITH_ERRORS -gt 0 ]]; then echo -e "${RED}$COLUMNS_WITH_ERRORS${NC}"; else echo "0"; fi)"

    # Display errors if any
    if [[ ${#ERRORS[@]} -gt 0 ]]; then
        echo
        echo -e "${BOLD}${RED}ERRORS ENCOUNTERED:${NC}"
        for error in "${ERRORS[@]}"; do
            echo -e "${RED}- $error${NC}"
        done
    fi

    echo
    if [[ ${#ERRORS[@]} -eq 0 ]]; then
        print_success "All operations completed successfully!"
    else
        print_warning "Conversion completed with some errors."
    fi
}

# Main function
main() {
    echo -e "${BOLD}MySQL UTF8MB4 Collation Converter${NC}"
    echo "=================================="
    echo

    # Parse command line arguments
    parse_arguments "$@"

    # Prompt for credentials if not provided
    prompt_for_credentials

    # Validate MySQL connection
    validate_mysql_connection

    # Get MySQL version and determine target collation
    get_mysql_version

    # Handle database selection
    if [[ -z "$DATABASE_NAME" ]]; then
        list_databases
    else
        validate_database
        print_info "Using database: $DATABASE_NAME"
    fi

    echo
    if [[ "$DRY_RUN" == "true" ]]; then
        print_warning "DRY RUN MODE: No changes will be made"
    else
        print_info "LIVE MODE: Changes will be applied to the database"
    fi

    echo
    read -p "Continue with the conversion? (y/N): " confirm
    if [[ ! "$confirm" =~ ^[Yy]$ ]]; then
        print_info "Operation cancelled by user"
        exit 0
    fi

    echo
    print_info "Starting conversion process..."

    # Process all tables
    process_tables

    # Display summary
    display_summary

    # Exit with appropriate code
    if [[ ${#ERRORS[@]} -gt 0 ]]; then
        exit 1
    else
        exit 0
    fi
}

# Check if script is being run directly
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi\t' read -r column_name column_type is_nullable column_default extra collation_name column_comment; do
                ((column_count++))
            done <<< "$columns_info"

            print_message "$CYAN" "⚙ Found $column_count columns needing conversion in $table_name"

            while IFS=
    done

    echo # New line after progress bar

    local script_end_time=$(date +%s)
    local total_duration=$((script_end_time - script_start_time))

    print_info "All tables processed in ${total_duration}s"
}

# Function to display summary
display_summary() {
    echo
    echo -e "${BOLD}=======================================${NC}"
    echo -e "${BOLD}         CONVERSION SUMMARY           ${NC}"
    echo -e "${BOLD}=======================================${NC}"

    echo -e "${BOLD}Database:${NC} $DATABASE_NAME"
    echo -e "${BOLD}Target Collation:${NC} $TARGET_COLLATION"
    echo -e "${BOLD}Mode:${NC} $(if [[ "$DRY_RUN" == "true" ]]; then echo "Dry Run"; else echo "Live Run"; fi)"
    echo

    echo -e "${BOLD}Tables Converted:${NC} ${GREEN}$TABLES_CONVERTED${NC}"
    echo -e "${BOLD}Tables Skipped:${NC} ${YELLOW}$TABLES_SKIPPED${NC}"
    echo -e "${BOLD}Tables With Errors:${NC} $(if [[ $TABLES_WITH_ERRORS -gt 0 ]]; then echo -e "${RED}$TABLES_WITH_ERRORS${NC}"; else echo "0"; fi)"
    echo

    echo -e "${BOLD}Columns Converted:${NC} ${GREEN}$COLUMNS_CONVERTED${NC}"
    echo -e "${BOLD}Columns With Errors:${NC} $(if [[ $COLUMNS_WITH_ERRORS -gt 0 ]]; then echo -e "${RED}$COLUMNS_WITH_ERRORS${NC}"; else echo "0"; fi)"

    # Display errors if any
    if [[ ${#ERRORS[@]} -gt 0 ]]; then
        echo
        echo -e "${BOLD}${RED}ERRORS ENCOUNTERED:${NC}"
        for error in "${ERRORS[@]}"; do
            echo -e "${RED}- $error${NC}"
        done
    fi

    echo
    if [[ ${#ERRORS[@]} -eq 0 ]]; then
        print_success "All operations completed successfully!"
    else
        print_warning "Conversion completed with some errors."
    fi
}

# Main function
main() {
    echo -e "${BOLD}MySQL UTF8MB4 Collation Converter${NC}"
    echo "=================================="
    echo

    # Parse command line arguments
    parse_arguments "$@"

    # Prompt for credentials if not provided
    prompt_for_credentials

    # Validate MySQL connection
    validate_mysql_connection

    # Get MySQL version and determine target collation
    get_mysql_version

    # Handle database selection
    if [[ -z "$DATABASE_NAME" ]]; then
        list_databases
    else
        validate_database
        print_info "Using database: $DATABASE_NAME"
    fi

    echo
    if [[ "$DRY_RUN" == "true" ]]; then
        print_warning "DRY RUN MODE: No changes will be made"
    else
        print_info "LIVE MODE: Changes will be applied to the database"
    fi

    echo
    read -p "Continue with the conversion? (y/N): " confirm
    if [[ ! "$confirm" =~ ^[Yy]$ ]]; then
        print_info "Operation cancelled by user"
        exit 0
    fi

    echo
    print_info "Starting conversion process..."

    # Process all tables
    process_tables

    # Display summary
    display_summary

    # Exit with appropriate code
    if [[ ${#ERRORS[@]} -gt 0 ]]; then
        exit 1
    else
        exit 0
    fi
}

# Check if script is being run directly
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi\t' read -r column_name column_type is_nullable column_default extra collation_name column_comment; do
                convert_column "$table_name" "$column_name" "$column_type" "$is_nullable" "$column_default" "$extra" "$collation_name" "$column_comment"
            done <<< "$columns_info"
        else
            print_message "$GREEN" "✓ All columns in $table_name already use correct collation"
        fi
    done

    echo # New line after progress bar

    local script_end_time=$(date +%s)
    local total_duration=$((script_end_time - script_start_time))

    print_info "All tables processed in ${total_duration}s"
}

# Function to display summary
display_summary() {
    echo
    echo -e "${BOLD}=======================================${NC}"
    echo -e "${BOLD}         CONVERSION SUMMARY           ${NC}"
    echo -e "${BOLD}=======================================${NC}"

    echo -e "${BOLD}Database:${NC} $DATABASE_NAME"
    echo -e "${BOLD}Target Collation:${NC} $TARGET_COLLATION"
    echo -e "${BOLD}Mode:${NC} $(if [[ "$DRY_RUN" == "true" ]]; then echo "Dry Run"; else echo "Live Run"; fi)"
    echo

    echo -e "${BOLD}Tables Converted:${NC} ${GREEN}$TABLES_CONVERTED${NC}"
    echo -e "${BOLD}Tables Skipped:${NC} ${YELLOW}$TABLES_SKIPPED${NC}"
    echo -e "${BOLD}Tables With Errors:${NC} $(if [[ $TABLES_WITH_ERRORS -gt 0 ]]; then echo -e "${RED}$TABLES_WITH_ERRORS${NC}"; else echo "0"; fi)"
    echo

    echo -e "${BOLD}Columns Converted:${NC} ${GREEN}$COLUMNS_CONVERTED${NC}"
    echo -e "${BOLD}Columns With Errors:${NC} $(if [[ $COLUMNS_WITH_ERRORS -gt 0 ]]; then echo -e "${RED}$COLUMNS_WITH_ERRORS${NC}"; else echo "0"; fi)"

    # Display errors if any
    if [[ ${#ERRORS[@]} -gt 0 ]]; then
        echo
        echo -e "${BOLD}${RED}ERRORS ENCOUNTERED:${NC}"
        for error in "${ERRORS[@]}"; do
            echo -e "${RED}- $error${NC}"
        done
    fi

    echo
    if [[ ${#ERRORS[@]} -eq 0 ]]; then
        print_success "All operations completed successfully!"
    else
        print_warning "Conversion completed with some errors."
    fi
}

# Main function
main() {
    echo -e "${BOLD}MySQL UTF8MB4 Collation Converter${NC}"
    echo "=================================="
    echo

    # Parse command line arguments
    parse_arguments "$@"

    # Prompt for credentials if not provided
    prompt_for_credentials

    # Validate MySQL connection
    validate_mysql_connection

    # Get MySQL version and determine target collation
    get_mysql_version

    # Handle database selection
    if [[ -z "$DATABASE_NAME" ]]; then
        list_databases
    else
        validate_database
        print_info "Using database: $DATABASE_NAME"
    fi

    echo
    if [[ "$DRY_RUN" == "true" ]]; then
        print_warning "DRY RUN MODE: No changes will be made"
    else
        print_info "LIVE MODE: Changes will be applied to the database"
    fi

    echo
    read -p "Continue with the conversion? (y/N): " confirm
    if [[ ! "$confirm" =~ ^[Yy]$ ]]; then
        print_info "Operation cancelled by user"
        exit 0
    fi

    echo
    print_info "Starting conversion process..."

    # Process all tables
    process_tables

    # Display summary
    display_summary

    # Exit with appropriate code
    if [[ ${#ERRORS[@]} -gt 0 ]]; then
        exit 1
    else
        exit 0
    fi
}

# Check if script is being run directly
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi\t' read -r index_name non_unique index_type; do
            local index_description="$index_name ($index_type"
            if [[ "$non_unique" == "0" ]]; then
                index_description="$index_description, UNIQUE"
            fi
            index_description="$index_description)"
            print_message "$YELLOW" "      - $index_description"
        done <<< "$indexes_info"
        print_message "$YELLOW" "    ⚠ Indexes should remain functional, but verify after conversion"
    fi
}
build_column_definition() {
    local column_type=$1
    local is_nullable=$2
    local column_default=$3
    local extra=$4
    local column_comment=$5

    local definition="$column_type CHARACTER SET utf8mb4 COLLATE $TARGET_COLLATION"

    # Handle NULL/NOT NULL
    if [[ "$is_nullable" == "NO" ]]; then
        definition="$definition NOT NULL"
    else
        definition="$definition NULL"
    fi

    # Handle DEFAULT values with proper escaping and special cases
    if [[ "$column_default" != "NULL" && -n "$column_default" ]]; then
        case "$column_default" in
            "CURRENT_TIMESTAMP"|"current_timestamp()"|"now()"|"CURRENT_TIMESTAMP()"|"NULL")
                definition="$definition DEFAULT $column_default"
                ;;
            *)
                # Escape single quotes and wrap in quotes for string defaults
                local escaped_default=$(printf '%s\n' "$column_default" | sed "s/'/''/g")
                definition="$definition DEFAULT '$escaped_default'"
                ;;
        esac
    fi

    # Handle EXTRA attributes (AUTO_INCREMENT, ON UPDATE, etc.)
    if [[ -n "$extra" && "$extra" != "NULL" ]]; then
        definition="$definition $extra"
    fi

    # Handle COMMENT
    if [[ -n "$column_comment" && "$column_comment" != "NULL" ]]; then
        local escaped_comment=$(printf '%s\n' "$column_comment" | sed "s/'/''/g")
        definition="$definition COMMENT '$escaped_comment'"
    fi

    echo "$definition"
}

# Function to convert column
convert_column() {
    local table_name=$1
    local column_name=$2
    local column_type=$3
    local is_nullable=$4
    local column_default=$5
    local extra=$6
    local current_collation=$7
    local column_comment=$8

    print_message "$CYAN" "  ⚙ Converting column: $column_name ($current_collation → $TARGET_COLLATION)" true

    if [[ "$DRY_RUN" == "true" ]]; then
        local column_def=$(build_column_definition "$column_type" "$is_nullable" "$column_default" "$extra" "$column_comment")
        print_message "$YELLOW" "  🔍 DRY RUN: Would execute: ALTER TABLE \`$DATABASE_NAME\`.\`$table_name\` MODIFY COLUMN \`$column_name\` $column_def"
        return 0
    fi

    # Get the complete column definition preserving all properties
    local column_definition=$(build_column_definition "$column_type" "$is_nullable" "$column_default" "$extra" "$column_comment")

    local alter_sql="ALTER TABLE \`$DATABASE_NAME\`.\`$table_name\` MODIFY COLUMN \`$column_name\` $column_definition;"

    if [[ "$VERBOSE" == "true" ]]; then
        print_message "$BLUE" "    SQL: $alter_sql"
    fi

    local start_time=$(date +%s)

    if mysql -h"$MYSQL_HOST" -P"$MYSQL_PORT" -u"$MYSQL_USER" -p"$MYSQL_PASSWORD" \
        -e "$alter_sql" 2>/tmp/mysql_column_error.log; then

        local end_time=$(date +%s)
        local duration=$((end_time - start_time))
        print_message "$GREEN" "  ✓ Column $column_name converted in ${duration}s"
        ((COLUMNS_CONVERTED++))
        return 0
    else
        local error_details=$(cat /tmp/mysql_column_error.log 2>/dev/null || echo "Unknown error")
        local error_msg="Failed to convert column $table_name.$column_name: $error_details"
        print_message "$RED" "  ❌ $error_msg"
        print_message "$RED" "    SQL was: $alter_sql"
        ERRORS+=("$error_msg")
        ((COLUMNS_WITH_ERRORS++))
        return 1
    fi
}

# Function to show progress bar
show_progress() {
    local current=$1
    local total=$2
    local table_name=$3
    local width=50

    local percentage=$((current * 100 / total))
    local completed=$((current * width / total))
    local remaining=$((width - completed))

    local bar=""
    for ((i=0; i<completed; i++)); do bar+="="; done
    for ((i=0; i<remaining; i++)); do bar+=" "; done

    # Truncate table name if too long
    local display_name="$table_name"
    if [[ ${#table_name} -gt 20 ]]; then
        display_name="${table_name:0:17}..."
    fi

    printf "\r[%s] %3d%% (%d/%d) - %-20s" "$bar" "$percentage" "$current" "$total" "$display_name"
}

# Function to process all tables
process_tables() {
    local tables_info
    tables_info=$(get_tables_needing_conversion)

    if [[ -z "$tables_info" ]]; then
        print_info "No tables found in database $DATABASE_NAME"
        return 0
    fi

    local total_tables=0
    local table_array=()

    # Count total tables and build array
    while IFS=$'\t' read -r table_name table_collation size_mb; do
        table_array+=("$table_name|$table_collation|$size_mb")
        ((total_tables++))
    done <<< "$tables_info"

    print_info "Found $total_tables tables to process"
    echo

    local current_table=0
    local script_start_time=$(date +%s)

    for table_info in "${table_array[@]}"; do
        IFS='|' read -r table_name table_collation size_mb <<< "$table_info"
        ((current_table++))

        # Show progress
        show_progress "$current_table" "$total_tables" "$table_name"

        if [[ "$VERBOSE" == "true" ]]; then
            echo # New line for verbose output
            print_info "Processing table $current_table of $total_tables: $table_name"
        fi

        # Check if table needs conversion
        if table_needs_conversion "$table_name"; then
            convert_table "$table_name" "$table_collation" "$size_mb"
        else
            print_message "$GREEN" "✓ Table $table_name (${size_mb}MB) already uses $TARGET_COLLATION - skipping"
            ((TABLES_SKIPPED++))
        fi

        # Process columns
        local columns_info
        columns_info=$(get_columns_needing_conversion "$table_name")

        if [[ -n "$columns_info" ]]; then
            local column_count=0
            while IFS=
    done

    echo # New line after progress bar

    local script_end_time=$(date +%s)
    local total_duration=$((script_end_time - script_start_time))

    print_info "All tables processed in ${total_duration}s"
}

# Function to display summary
display_summary() {
    echo
    echo -e "${BOLD}=======================================${NC}"
    echo -e "${BOLD}         CONVERSION SUMMARY           ${NC}"
    echo -e "${BOLD}=======================================${NC}"

    echo -e "${BOLD}Database:${NC} $DATABASE_NAME"
    echo -e "${BOLD}Target Collation:${NC} $TARGET_COLLATION"
    echo -e "${BOLD}Mode:${NC} $(if [[ "$DRY_RUN" == "true" ]]; then echo "Dry Run"; else echo "Live Run"; fi)"
    echo

    echo -e "${BOLD}Tables Converted:${NC} ${GREEN}$TABLES_CONVERTED${NC}"
    echo -e "${BOLD}Tables Skipped:${NC} ${YELLOW}$TABLES_SKIPPED${NC}"
    echo -e "${BOLD}Tables With Errors:${NC} $(if [[ $TABLES_WITH_ERRORS -gt 0 ]]; then echo -e "${RED}$TABLES_WITH_ERRORS${NC}"; else echo "0"; fi)"
    echo

    echo -e "${BOLD}Columns Converted:${NC} ${GREEN}$COLUMNS_CONVERTED${NC}"
    echo -e "${BOLD}Columns With Errors:${NC} $(if [[ $COLUMNS_WITH_ERRORS -gt 0 ]]; then echo -e "${RED}$COLUMNS_WITH_ERRORS${NC}"; else echo "0"; fi)"

    # Display errors if any
    if [[ ${#ERRORS[@]} -gt 0 ]]; then
        echo
        echo -e "${BOLD}${RED}ERRORS ENCOUNTERED:${NC}"
        for error in "${ERRORS[@]}"; do
            echo -e "${RED}- $error${NC}"
        done
    fi

    echo
    if [[ ${#ERRORS[@]} -eq 0 ]]; then
        print_success "All operations completed successfully!"
    else
        print_warning "Conversion completed with some errors."
    fi
}

# Main function
main() {
    echo -e "${BOLD}MySQL UTF8MB4 Collation Converter${NC}"
    echo "=================================="
    echo

    # Parse command line arguments
    parse_arguments "$@"

    # Prompt for credentials if not provided
    prompt_for_credentials

    # Validate MySQL connection
    validate_mysql_connection

    # Get MySQL version and determine target collation
    get_mysql_version

    # Handle database selection
    if [[ -z "$DATABASE_NAME" ]]; then
        list_databases
    else
        validate_database
        print_info "Using database: $DATABASE_NAME"
    fi

    echo
    if [[ "$DRY_RUN" == "true" ]]; then
        print_warning "DRY RUN MODE: No changes will be made"
    else
        print_info "LIVE MODE: Changes will be applied to the database"
    fi

    echo
    read -p "Continue with the conversion? (y/N): " confirm
    if [[ ! "$confirm" =~ ^[Yy]$ ]]; then
        print_info "Operation cancelled by user"
        exit 0
    fi

    echo
    print_info "Starting conversion process..."

    # Process all tables
    process_tables

    # Display summary
    display_summary

    # Exit with appropriate code
    if [[ ${#ERRORS[@]} -gt 0 ]]; then
        exit 1
    else
        exit 0
    fi
}

# Check if script is being run directly
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi\t' read -r column_name column_type is_nullable column_default extra collation_name column_comment; do
                ((column_count++))
            done <<< "$columns_info"

            print_message "$CYAN" "⚙ Found $column_count columns needing conversion in $table_name"

            while IFS=
    done

    echo # New line after progress bar

    local script_end_time=$(date +%s)
    local total_duration=$((script_end_time - script_start_time))

    print_info "All tables processed in ${total_duration}s"
}

# Function to display summary
display_summary() {
    echo
    echo -e "${BOLD}=======================================${NC}"
    echo -e "${BOLD}         CONVERSION SUMMARY           ${NC}"
    echo -e "${BOLD}=======================================${NC}"

    echo -e "${BOLD}Database:${NC} $DATABASE_NAME"
    echo -e "${BOLD}Target Collation:${NC} $TARGET_COLLATION"
    echo -e "${BOLD}Mode:${NC} $(if [[ "$DRY_RUN" == "true" ]]; then echo "Dry Run"; else echo "Live Run"; fi)"
    echo

    echo -e "${BOLD}Tables Converted:${NC} ${GREEN}$TABLES_CONVERTED${NC}"
    echo -e "${BOLD}Tables Skipped:${NC} ${YELLOW}$TABLES_SKIPPED${NC}"
    echo -e "${BOLD}Tables With Errors:${NC} $(if [[ $TABLES_WITH_ERRORS -gt 0 ]]; then echo -e "${RED}$TABLES_WITH_ERRORS${NC}"; else echo "0"; fi)"
    echo

    echo -e "${BOLD}Columns Converted:${NC} ${GREEN}$COLUMNS_CONVERTED${NC}"
    echo -e "${BOLD}Columns With Errors:${NC} $(if [[ $COLUMNS_WITH_ERRORS -gt 0 ]]; then echo -e "${RED}$COLUMNS_WITH_ERRORS${NC}"; else echo "0"; fi)"

    # Display errors if any
    if [[ ${#ERRORS[@]} -gt 0 ]]; then
        echo
        echo -e "${BOLD}${RED}ERRORS ENCOUNTERED:${NC}"
        for error in "${ERRORS[@]}"; do
            echo -e "${RED}- $error${NC}"
        done
    fi

    echo
    if [[ ${#ERRORS[@]} -eq 0 ]]; then
        print_success "All operations completed successfully!"
    else
        print_warning "Conversion completed with some errors."
    fi
}

# Main function
main() {
    echo -e "${BOLD}MySQL UTF8MB4 Collation Converter${NC}"
    echo "=================================="
    echo

    # Parse command line arguments
    parse_arguments "$@"

    # Prompt for credentials if not provided
    prompt_for_credentials

    # Validate MySQL connection
    validate_mysql_connection

    # Get MySQL version and determine target collation
    get_mysql_version

    # Handle database selection
    if [[ -z "$DATABASE_NAME" ]]; then
        list_databases
    else
        validate_database
        print_info "Using database: $DATABASE_NAME"
    fi

    echo
    if [[ "$DRY_RUN" == "true" ]]; then
        print_warning "DRY RUN MODE: No changes will be made"
    else
        print_info "LIVE MODE: Changes will be applied to the database"
    fi

    echo
    read -p "Continue with the conversion? (y/N): " confirm
    if [[ ! "$confirm" =~ ^[Yy]$ ]]; then
        print_info "Operation cancelled by user"
        exit 0
    fi

    echo
    print_info "Starting conversion process..."

    # Process all tables
    process_tables

    # Display summary
    display_summary

    # Exit with appropriate code
    if [[ ${#ERRORS[@]} -gt 0 ]]; then
        exit 1
    else
        exit 0
    fi
}

# Check if script is being run directly
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi\t' read -r column_name column_type is_nullable column_default extra collation_name column_comment; do
                convert_column "$table_name" "$column_name" "$column_type" "$is_nullable" "$column_default" "$extra" "$collation_name" "$column_comment"
            done <<< "$columns_info"
        else
            print_message "$GREEN" "✓ All columns in $table_name already use correct collation"
        fi
    done

    echo # New line after progress bar

    local script_end_time=$(date +%s)
    local total_duration=$((script_end_time - script_start_time))

    print_info "All tables processed in ${total_duration}s"
}

# Function to display summary
display_summary() {
    echo
    echo -e "${BOLD}=======================================${NC}"
    echo -e "${BOLD}         CONVERSION SUMMARY           ${NC}"
    echo -e "${BOLD}=======================================${NC}"

    echo -e "${BOLD}Database:${NC} $DATABASE_NAME"
    echo -e "${BOLD}Target Collation:${NC} $TARGET_COLLATION"
    echo -e "${BOLD}Mode:${NC} $(if [[ "$DRY_RUN" == "true" ]]; then echo "Dry Run"; else echo "Live Run"; fi)"
    echo

    echo -e "${BOLD}Tables Converted:${NC} ${GREEN}$TABLES_CONVERTED${NC}"
    echo -e "${BOLD}Tables Skipped:${NC} ${YELLOW}$TABLES_SKIPPED${NC}"
    echo -e "${BOLD}Tables With Errors:${NC} $(if [[ $TABLES_WITH_ERRORS -gt 0 ]]; then echo -e "${RED}$TABLES_WITH_ERRORS${NC}"; else echo "0"; fi)"
    echo

    echo -e "${BOLD}Columns Converted:${NC} ${GREEN}$COLUMNS_CONVERTED${NC}"
    echo -e "${BOLD}Columns With Errors:${NC} $(if [[ $COLUMNS_WITH_ERRORS -gt 0 ]]; then echo -e "${RED}$COLUMNS_WITH_ERRORS${NC}"; else echo "0"; fi)"

    # Display errors if any
    if [[ ${#ERRORS[@]} -gt 0 ]]; then
        echo
        echo -e "${BOLD}${RED}ERRORS ENCOUNTERED:${NC}"
        for error in "${ERRORS[@]}"; do
            echo -e "${RED}- $error${NC}"
        done
    fi

    echo
    if [[ ${#ERRORS[@]} -eq 0 ]]; then
        print_success "All operations completed successfully!"
    else
        print_warning "Conversion completed with some errors."
    fi
}

# Main function
main() {
    echo -e "${BOLD}MySQL UTF8MB4 Collation Converter${NC}"
    echo "=================================="
    echo

    # Parse command line arguments
    parse_arguments "$@"

    # Prompt for credentials if not provided
    prompt_for_credentials

    # Validate MySQL connection
    validate_mysql_connection

    # Get MySQL version and determine target collation
    get_mysql_version

    # Handle database selection
    if [[ -z "$DATABASE_NAME" ]]; then
        list_databases
    else
        validate_database
        print_info "Using database: $DATABASE_NAME"
    fi

    echo
    if [[ "$DRY_RUN" == "true" ]]; then
        print_warning "DRY RUN MODE: No changes will be made"
    else
        print_info "LIVE MODE: Changes will be applied to the database"
    fi

    echo
    read -p "Continue with the conversion? (y/N): " confirm
    if [[ ! "$confirm" =~ ^[Yy]$ ]]; then
        print_info "Operation cancelled by user"
        exit 0
    fi

    echo
    print_info "Starting conversion process..."

    # Process all tables
    process_tables

    # Display summary
    display_summary

    # Exit with appropriate code
    if [[ ${#ERRORS[@]} -gt 0 ]]; then
        exit 1
    else
        exit 0
    fi
}

# Check if script is being run directly
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi
