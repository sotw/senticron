#!/bin/bash

usage() {
    echo "Usage: $0 \"command\" [time] [priority] [redirect]"
    echo ""
    echo "Add a one-time job to crontab.txt"
    echo ""
    echo "Arguments:"
    echo "  command    The command to run (required, wrap in quotes)"
    echo "  time       Time to run (optional, default: 10 seconds from now)"
    echo "             Formats:"
    echo "               - HH:MM        (today at HH:MM)"
    echo "               - YYYY-MM-DD HH:MM  (specific date)"
    echo "               - +N           (N seconds from now)"
    echo "  priority   Priority 1-10 (default: 5)"
    echo "  redirect   true/false to redirect output (default: true)"
    echo ""
    echo "Examples:"
    echo "  $0 \"echo hello\"                    # runs in 10 seconds"
    echo "  $0 \"echo hello\" \"14:30\"           # runs today at 14:30"
    echo "  $0 \"echo hello\" \"2025-05-21 09:00\" 5 true"
    echo "  $0 \"python3 script.py\" \"+30\" 3"
    exit 1
}

CRONTAB_FILE="crontab.txt"

if [ ! -f "$CRONTAB_FILE" ]; then
    echo "Error: $CRONTAB_FILE not found"
    exit 1
fi

if [ $# -lt 1 ]; then
    usage
fi

COMMAND="$1"
TIME_ARG="${2:-}"
PRIORITY="${3:-5}"
REDIRECT="${4:-false}"

if [ -z "$TIME_ARG" ]; then
    RUN_TIME=$(date -d "+10 seconds" "+%M %H %d %m *")
    echo "No time specified, scheduling 10 seconds from now..."
elif [[ "$TIME_ARG" =~ ^\+[0-9]+$ ]]; then
    SECONDS=${TIME_ARG#+}
    RUN_TIME=$(date -d "+$SECONDS seconds" "+%M %H %d %m *")
    echo "Scheduling $SECONDS seconds from now..."
elif [[ "$TIME_ARG" =~ ^[0-9]{2}:[0-9]{2}$ ]]; then
    RUN_TIME=$(date -d "$TIME_ARG" "+%M %H %d %m *")
    echo "Scheduling today at $TIME_ARG..."
elif [[ "$TIME_ARG" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}\ [0-9]{2}:[0-9]{2}$ ]]; then
    RUN_TIME=$(date -d "$TIME_ARG" "+%M %H %d %m %w")
    echo "Scheduling at $TIME_ARG..."
else
    echo "Error: Invalid time format. Use HH:MM, YYYY-MM-DD HH:MM, or +N"
    exit 1
fi

if ! [[ "$PRIORITY" =~ ^[1-9]$|^10$ ]]; then
    echo "Error: Priority must be 1-10"
    exit 1
fi

REDIRECT_LOWER=$(echo "$REDIRECT" | tr '[:upper:]' '[:lower:]')
if [[ "$REDIRECT_LOWER" == "false" || "$REDIRECT_LOWER" == "0" ]]; then
    REDIRECT="true"
else
    REDIRECT="false"
fi

ONETIME_ID=$(date +%s%N)

echo "# ONETIME:$ONETIME_ID" >> "$CRONTAB_FILE"
echo "$RUN_TIME $COMMAND $PRIORITY $REDIRECT" >> "$CRONTAB_FILE"

echo ""
echo "One-time job added:"
echo "  ID:        $ONETIME_ID"
echo "  Command:   $COMMAND"
echo "  Schedule:  $RUN_TIME"
echo "  Priority:  $PRIORITY"
echo "  Redirect:  $REDIRECT"
echo ""
echo "SentiCron will auto-remove this entry after execution."
