#!/bin/bash
#
# (C) Oliver Reiche <oliver.reiche@siemens.com>

# Source path to backup
conf_sourcepath="/"

# Destination path for storing the backup
conf_backuppath="/backup"

# Exclude paths from backup (relative to source path)
conf_excludepaths=("dev" "mnt" "tmp")

# Name of the snapshots to create (containing incremental backups)
conf_name="day"

# Number of snapshots to create
conf_snapshots="6"

# Further stages for storing older snapshots
conf_stages=(
    #NAME      MINUTES  SNAPSHOTS
    "week"      "10080" "3"
    "month"     "40320" "2"
    "quarter"  "120960" "1"
    "halfyear" "241920" "1"
    "year"     "483840" "3"
)

################################################################################

g_timestamp=$(date +%s)

function checkTimestamp() {
    local curr=${conf_stages[$1]}
    local minutes=${conf_stages[$(($1+1))]}
    local timestamp=$(cat $conf_backuppath/$curr.stamp 2>/dev/null)
    local retval=1

    if [ "$timestamp" == "" ]; then
        echo $g_timestamp > $conf_backuppath/$curr.stamp
    elif [ $((($(date +%s) - $timestamp) / 60)) -ge $minutes ]; then
        echo $g_timestamp > $conf_backuppath/$curr.stamp
        retval=0
    fi

    return $retval
}

################################################################################

function createStage() {
    local curr=${conf_stages[$1]}
    local prev
    local last

    if [ $1 -ge 3 ]; then
        prev=${conf_stages[$(($1-3))]}
    else
        prev=$conf_name
    fi
    last="$(ls $conf_backuppath/ | grep $prev\.[[:digit:]] | \
            sort -n -t '.' -k 2 | tail -n1)"

    if [ "$last" != "" ] && 
       [ "$last" != $prev.0 ]; then
        echo "Creating new snapshot for stage '$curr'."
        rm -rf $conf_backuppath/$curr.1
        mv $conf_backuppath/$last $conf_backuppath/$curr.1 2>/dev/null
    fi
}

################################################################################

function shiftStage() {
    local curr=${conf_stages[$1]}
    local i=${conf_stages[$(($1+2))]}

    if [ -d $conf_backuppath/$curr.1 ]; then
        echo "Shifting snapshots of stage '$curr'."
        rm -rf $conf_backuppath/$curr.$i
        while [ $i -gt 1 ]; do
            mv $conf_backuppath/$curr.$(($i-1)) \
                $conf_backuppath/$curr.$i 2>/dev/null
            i=$(($i-1))
        done
    fi
}

################################################################################

function createInit() {
    echo "Creating new snapshot for initial stage '$conf_name'."

    if [ -d $conf_backuppath/$conf_name.tmp ]; then
        # Reuse the temp directory
        mv $conf_backuppath/$conf_name.0 $conf_backuppath/$conf_name.1
        mv $conf_backuppath/$conf_name.tmp $conf_backuppath/$conf_name.0
        if [[ $OSTYPE == *darwin* ]]; then
            cd $conf_backuppath/$conf_name.1
            find . -print | cpio -pdlm $conf_backuppath/$conf_name.0 2>/dev/null
        else
            cp -al $conf_backuppath/$conf_name.1/. $conf_backuppath/$conf_name.0
        fi
    elif [ -d $conf_backuppath/$conf_name.0 ]; then
        rm -rf $conf_backuppath/$conf_name.1
        if [[ $OSTYPE == *darwin* ]]; then
            cd $conf_backuppath/$conf_name.0
            find . -print | cpio -pdlm $conf_backuppath/$conf_name.1 2>/dev/null
        else
            cp -al $conf_backuppath/$conf_name.0 $conf_backuppath/$conf_name.1
        fi
    fi

    local excluded
    local exclude=$(echo $conf_backuppath | grep $conf_sourcepath | \
                    sed "s/^$(echo $conf_sourcepath | sed 's/\//\\\//g')\///g")
    if [ "$exclude" != "" ]; then
        excluded="$excluded --exclude=$exclude"
    fi
    for exclude in "${conf_excludepaths[@]}"; do
        excluded="$excluded --exclude=$exclude"
    done

    rsync -a --delete $excluded $conf_sourcepath/ $conf_backuppath/$conf_name.0/
}

################################################################################

function shiftInit() {
    if [ -d $conf_backuppath/$conf_name.0 ]; then
        local i=$conf_snapshots

        echo "Shifting snapshots of initial stage '$conf_name'."

        if [ -d $conf_backuppath/$conf_name.$i ]; then
            # Store as temp, this speeds up the whole operation
            mv $conf_backuppath/$conf_name.$i $conf_backuppath/$conf_name.tmp
        fi

        while [ $i -gt 1 ]; do
            mv $conf_backuppath/$conf_name.$(($i-1)) \
                $conf_backuppath/$conf_name.$i 2>/dev/null
            i=$(($i-1))
        done
    fi
}

################################################################################

function main() {
    local n=${#conf_stages[*]}
    local retval=1

    if [ $(($n % 3)) -ne 0 ]; then
        echo "Configuration error: Malformed stages array."
    elif [ ! -d $conf_sourcepath ]; then
        echo "Directory '$conf_sourcepath' does not exist."
    elif [ ! -d $conf_backuppath ]; then
        echo "Directory '$conf_backuppath' does not exist."
    else
        local i=$(($n - 3))
        while [ $i -ge 0 ]; do
            checkTimestamp $i
            if [ "$?" == "0" ]; then
                shiftStage $i
                createStage $i
            fi
            i=$(($i-3))
        done

        shiftInit
        createInit

        echo "Finished backup process."
        retval=0
    fi

    echo

    exit $retval
}

main

################################################################################
################################################################################

