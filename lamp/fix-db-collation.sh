#!/bin/bash

# Script: fix-db-collation.sh
# Description: Updates MySQL database tables and columns to utf8mb4_general_ci collation
# Usage: ./fix-db-collation.sh


# To use this script:
# sudo -s
# cd /tmp
# wget -O ./lamp-setup.sh https://raw.githubusercontent.com/deforay/utility-scripts/master/lamp/fix-db-collation.sh
# chmod +x ./fix-db-collation.sh
# ./fix-db-collation.sh

# Text styling
BOLD="\033[1m"
NORMAL="\033[0m"
GREEN="\033[32m"
YELLOW="\033[33m"
BLUE="\033[34m"
RED="\033[31m"

# Progress bar function
show_progress() {
    local total=$1
    local current=$2
    local width=50
    local percentage=$((current * 100 / total))
    local completed=$((width * current / total))
    local remaining=$((width - completed))

    printf "\r[%${completed}s%${remaining}s] %d%%" "$(printf "%0.s#" $(seq 1 $completed))" "$(printf "%0.s-" $(seq 1 $remaining))" "$percentage"
}

# Banner
echo -e "${BOLD}${BLUE}==============================================${NORMAL}"
echo -e "${BOLD}${BLUE}  MySQL Database Collation Converter Tool    ${NORMAL}"
echo -e "${BOLD}${BLUE}  Convert to utf8mb4_general_ci              ${NORMAL}"
echo -e "${BOLD}${BLUE}==============================================${NORMAL}"
echo

# Prompt for MySQL connection details
read -p "$(echo -e ${BOLD})Enter MySQL Hostname [localhost]: $(echo -e ${NORMAL})" MYSQL_HOST
MYSQL_HOST=${MYSQL_HOST:-localhost}

read -p "$(echo -e ${BOLD})Enter MySQL Port [3306]: $(echo -e ${NORMAL})" MYSQL_PORT
MYSQL_PORT=${MYSQL_PORT:-3306}

read -p "$(echo -e ${BOLD})Enter MySQL Admin Username [root]: $(echo -e ${NORMAL})" MYSQL_USER
MYSQL_USER=${MYSQL_USER:-root}

read -p "$(echo -e ${BOLD})Enter MySQL Password []: $(echo -e ${NORMAL})" -s MYSQL_PASS
echo

# Create temporary MySQL config file to avoid password warnings
MYSQL_CNF=$(mktemp)
chmod 600 "$MYSQL_CNF"  # Secure the config file
cat > "$MYSQL_CNF" << EOF
[client]
host=$MYSQL_HOST
port=$MYSQL_PORT
user=$MYSQL_USER
password=$MYSQL_PASS
EOF

# Test connection
echo -e "\n${YELLOW}Testing connection to MySQL...${NORMAL}"
if ! mysql --defaults-file="$MYSQL_CNF" -e "SELECT 1" >/dev/null 2>&1; then
    echo -e "${BOLD}Connection failed! Please check your credentials and try again.${NORMAL}"
    rm "$MYSQL_CNF"
    exit 1
fi
echo -e "${GREEN}Connection successful!${NORMAL}\n"

# Get list of databases
echo -e "${YELLOW}Retrieving list of databases...${NORMAL}"
DATABASES=$(mysql --defaults-file="$MYSQL_CNF" -e "SHOW DATABASES;" | grep -v "Database\|information_schema\|performance_schema\|mysql\|sys")

# Display databases with numbers
echo -e "${BOLD}Available databases:${NORMAL}"
DB_ARRAY=()
i=1
for db in $DATABASES; do
    DB_ARRAY+=("$db")
    echo "$i) $db"
    ((i++))
done

# Prompt user to select databases
echo -e "\n${BOLD}Enter the numbers of databases to convert (comma or space-separated, e.g., '1 3 5' or '1,3,5'):${NORMAL}"
read -p "> " SELECTED_DBS

# Replace commas with spaces for consistent processing
SELECTED_DBS=${SELECTED_DBS//,/ }

# Validate input and create array of selected databases
SELECTED_DB_ARRAY=()
for num in $SELECTED_DBS; do
    if [[ "$num" =~ ^[0-9]+$ ]] && [ "$num" -ge 1 ] && [ "$num" -le "${#DB_ARRAY[@]}" ]; then
        SELECTED_DB_ARRAY+=("${DB_ARRAY[$num-1]}")
    else
        echo -e "${BOLD}Invalid selection: $num${NORMAL}"
    fi
done

if [ ${#SELECTED_DB_ARRAY[@]} -eq 0 ]; then
    echo -e "${BOLD}No valid databases selected. Exiting.${NORMAL}"
    exit 1
fi

# Confirm user selection
echo -e "\n${BOLD}You've selected the following databases to convert:${NORMAL}"
for db in "${SELECTED_DB_ARRAY[@]}"; do
    echo "- $db"
done

# Auto-confirm after timeout (10 seconds)
echo -e "$(echo -e ${BOLD})Continue with conversion? [Y/n] (Auto-yes in 10s): $(echo -e ${NORMAL})\c"
read -t 10 CONFIRM
if [[ "$CONFIRM" =~ ^[Nn]$ ]]; then
    echo -e "${BOLD}Operation canceled.${NORMAL}"
    exit 0
fi
echo -e "${YELLOW}Proceeding with conversion...${NORMAL}"

# Create SQL script for each database
for db in "${SELECTED_DB_ARRAY[@]}"; do
    echo -e "\n${YELLOW}Processing database: ${BOLD}$db${NORMAL}"

    # Create temporary SQL file
    SQL_FILE="/tmp/convert_${db}_collation.sql"
    > "$SQL_FILE"

    # Add database character set and collation conversion
    echo "ALTER DATABASE \`$db\` CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;" >> "$SQL_FILE"

    # Get all tables
    TABLES=$(mysql --defaults-file="$MYSQL_CNF" -e "SHOW TABLES FROM \`$db\`;" | grep -v "Tables_in")
    TABLES_ARRAY=($TABLES)
    TABLE_COUNT=${#TABLES_ARRAY[@]}

    echo -e "${YELLOW}Found ${TABLE_COUNT} tables in database ${BOLD}$db${NORMAL}"

    # Process each table into separate SQL files for better error tracking
    table_index=0
    for table in $TABLES; do
        # Update progress
        ((table_index++))
        show_progress $TABLE_COUNT $table_index

        # Create a separate SQL file for each table
        TABLE_SQL_FILE="/tmp/convert_${db}_${table}_collation.sql"
        > "$TABLE_SQL_FILE"

        # Convert table to utf8mb4
        echo "ALTER TABLE \`$db\`.\`$table\` CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;" >> "$TABLE_SQL_FILE"

        # Get all columns for the table
        COLUMNS=$(mysql --defaults-file="$MYSQL_CNF" -e "SHOW COLUMNS FROM \`$db\`.\`$table\`;" | awk '{print $1}' | grep -v "Field")

        # For each text/string column, alter it specifically
        for column in $COLUMNS; do
            # Get column type
            COLUMN_TYPE=$(mysql --defaults-file="$MYSQL_CNF" -e "SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='$db' AND TABLE_NAME='$table' AND COLUMN_NAME='$column';" | grep -v "DATA_TYPE")

            # Check if column is a string type that needs explicit collation
            if [[ "$COLUMN_TYPE" =~ ^(char|varchar|text|tinytext|mediumtext|longtext|enum|set)$ ]]; then
                # Get current column definition
                COLUMN_DEF=$(mysql --defaults-file="$MYSQL_CNF" -e "SHOW COLUMNS FROM \`$db\`.\`$table\` WHERE Field='$column';" | grep -v "Field" | awk '{print $2}')

                # Add ALTER statement for this column
                echo "ALTER TABLE \`$db\`.\`$table\` MODIFY COLUMN \`$column\` $COLUMN_DEF CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;" >> "$TABLE_SQL_FILE"
            fi
        done

        # Append the table's SQL to the main SQL file
        cat "$TABLE_SQL_FILE" >> "$SQL_FILE"

        # Clean up the table's SQL file
        rm "$TABLE_SQL_FILE"
    done

    # Complete the progress bar
    echo -e "\n"

    # Execute the SQL file for database-level changes first
    echo -e "${YELLOW}Setting database character set for: ${BOLD}$db${NORMAL}"
    echo "ALTER DATABASE \`$db\` CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;" > "/tmp/convert_${db}_db_collation.sql"
    mysql --defaults-file="$MYSQL_CNF" < "/tmp/convert_${db}_db_collation.sql" 2> /tmp/mysql_error.log
    rm "/tmp/convert_${db}_db_collation.sql"

    # Process each table individually for better error tracking
    echo -e "${YELLOW}Processing tables for database: ${BOLD}$db${NORMAL}"
    for table in $TABLES; do
        echo -e "  - Processing table: ${BOLD}$table${NORMAL}"

        # Extract just this table's SQL from the main file
        grep "\`$db\`.\`$table\`" "$SQL_FILE" > "/tmp/convert_${db}_${table}_exec.sql"

        # Execute SQL for this table
        if ! mysql --defaults-file="$MYSQL_CNF" < "/tmp/convert_${db}_${table}_exec.sql" 2> /tmp/mysql_error.log; then
            echo -e "\n${RED}${BOLD}Error occurred while processing table $db.$table${NORMAL}"
            echo -e "${RED}$(cat /tmp/mysql_error.log)${NORMAL}"
            echo -e "${YELLOW}Stopping execution. Previous tables and databases may have been modified.${NORMAL}"

            # Clean up
            rm -f "/tmp/convert_${db}_${table}_exec.sql" "$SQL_FILE" "$MYSQL_CNF" /tmp/mysql_error.log
            exit 1
        fi

        # Clean up table SQL file
        rm -f "/tmp/convert_${db}_${table}_exec.sql"
    done

    if [ $? -eq 0 ]; then
        echo -e "${GREEN}Successfully converted database $db to utf8mb4_general_ci!${NORMAL}"
    else
        echo -e "${BOLD}Error converting database $db.${NORMAL}"
    fi

    # Clean up
    rm "$SQL_FILE"
done

# Clean up the temporary MySQL config file
rm "$MYSQL_CNF"

echo -e "\n${GREEN}${BOLD}Conversion completed!${NORMAL}"
echo -e "All selected databases have been processed."
