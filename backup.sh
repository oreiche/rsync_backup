#!/bin/bash
#
# (C) Oliver Reiche <oliver.reiche@siemens.com>

# Source path to backup
conf_sourcepath="/"

# Destination path for storing the backup
conf_backuppath="/backup"

# Exclude paths from backup (relative to source path)
conf_excludepaths=("dev" "mnt" "tmp")

# Stages for storing snapshots (ascending interval length)
conf_stages=(
    #NAME      MINUTES  COUNT
    "day"        "1440" "6"
    "week"      "10080" "3"
    "month"     "40320" "2"
    "quarter"  "120960" "1"
    "halfyear" "241920" "1"
    "year"     "483840" "3"
)

################################################################################

## Time stamp of initial script execution (@see checkTimestamp())
g_timestamp=$(date +%s)

##
## @brief Checks whether time stamp of stage is exceeded. Creates new time stamp
##        for current stage if time stamp was exceeded or didn't exist.
## @param  {Number} $1  Index of the stage in stages array.
## @retval {String} "0" Time stamp was exceeded.
## @retval {String} "1" Time stamp was not exceeded.
##
function checkTimestamp() {
    local curr=${conf_stages[$1]}
    local seconds=$((${conf_stages[$(($1+1))]} * 60))
    local timestamp=$(cat $conf_backuppath/$curr.stamp 2>/dev/null)
    local retval=1

    if [ "$timestamp" != "" ]; then
        local delta=$(($(date +%s) - $timestamp))
        if [ $delta -ge $seconds ]; then
            # Incrementing existing time stamp by $seconds*n (n >= 1)
            echo $(($timestamp + ((($delta / $seconds) + 1) * $seconds))) \
                > $conf_backuppath/$curr.stamp
            retval=0
        fi
    else
        # Creating initial time stamp
        echo $g_timestamp > $conf_backuppath/$curr.stamp
    fi

    return $retval
}

################################################################################

##
## @brief Create a new snapshot for current stage. Searches oldest snapshot of
##        previous stage and moves it this stage.
## @param {Number} $1 Index of the current stage in stages array.
##
function createStage() {
    local curr=${conf_stages[$1]}
    local prev=${conf_stages[$(($1-3))]}
    local last="$(ls $conf_backuppath/ | grep $prev\.[[:digit:]] | \
                  sort -n -t '.' -k 2 | tail -n1)"

    if [ "$last" != "" ] && 
       [ "$last" != $prev.0 ]; then
        echo "Creating new snapshot for stage '$curr'."
        rm -rf $conf_backuppath/$curr.1
        mv $conf_backuppath/$last $conf_backuppath/$curr.1 2>/dev/null
    fi
}

################################################################################

##
## @brief Shifts all snapshots of current stage. The oldest snapshot will be
##        deleted.
## @param {Number} $1 Index of the current stage in stages array.
##
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

##
## @brief Creates a new snapshot for initial stage. A previous shifting process
##        might has reserved a temp directory which will be used to speed up the
##        creation process.
##
function createInit() {
    local init=${conf_stages[0]}

    echo "Creating new snapshot for initial stage '$init'."

    if [ -d $conf_backuppath/$init.tmp ]; then
        # Reuse the temp directory
        mv $conf_backuppath/$init.0 $conf_backuppath/$init.1
        mv $conf_backuppath/$init.tmp $conf_backuppath/$init.0
        if [[ $OSTYPE == *darwin* ]]; then
            cd $conf_backuppath/$init.1
            find . -print | cpio -pdlm $conf_backuppath/$init.0 2>/dev/null
        else
            cp -al $conf_backuppath/$init.1/. $conf_backuppath/$init.0
        fi
    elif [ -d $conf_backuppath/$init.0 ]; then
        rm -rf $conf_backuppath/$init.1
        if [[ $OSTYPE == *darwin* ]]; then
            cd $conf_backuppath/$init.0
            find . -print | cpio -pdlm $conf_backuppath/$init.1 2>/dev/null
        else
            cp -al $conf_backuppath/$init.0 $conf_backuppath/$init.1
        fi
    fi

    local excluded
    local exclude=$(echo $conf_backuppath | grep $conf_sourcepath | \
                    sed "s/^$(echo $conf_sourcepath | sed 's/\//\\\//g')\/*//g")
    if [ "$exclude" != "" ]; then
        excluded="$excluded --exclude=$exclude"
    fi
    for exclude in "${conf_excludepaths[@]}"; do
        excluded="$excluded --exclude=$exclude"
    done

    rsync -a --delete $excluded $conf_sourcepath/ $conf_backuppath/$init.0/
}

################################################################################

##
## @brief Shifts all snapshots of the initial stage. The oldest snapshot might
##        be stored in temp directory to be used by the consecutive create
##        function.
##
function shiftInit() {
    local init=${conf_stages[0]}
    local i=${conf_stages[2]}

    if [ -d $conf_backuppath/$init.0 ]; then
        echo "Shifting snapshots of initial stage '$init'."

        if [ -d $conf_backuppath/$init.$i ]; then
            # Store as temp, this speeds up the whole operation
            mv $conf_backuppath/$init.$i $conf_backuppath/$init.tmp
        fi

        while [ $i -gt 1 ]; do
            mv $conf_backuppath/$init.$(($i-1)) \
                $conf_backuppath/$init.$i 2>/dev/null
            i=$(($i-1))
        done
    fi
}

################################################################################

##
## @brief Main backup function. Checks configuration parameters and starts to
##        execute stages in reverse order. The last stage well always be the so
##        called 'initial stage'.
## @retval {String} "0" Configuration is valid and working paths do exist.
## @retval {String} "1" Configuration is invalid or working paths don't exist.
##
function main() {
    local n=${#conf_stages[*]}
    local retval=1

    if [ $n -lt 3 ] ||
       [ $(($n % 3)) -ne 0 ]; then
        echo "Configuration error: Malformed stages array."
    elif [ ! -d $conf_sourcepath ]; then
        echo "Directory '$conf_sourcepath' does not exist."
    elif [ ! -d $conf_backuppath ]; then
        echo "Directory '$conf_backuppath' does not exist."
    else
        echo "Starting backup of '$conf_sourcepath'."

        local i=$(($n - 3))
        while [ $i -ge 3 ]; do
            checkTimestamp $i
            if [ "$?" == "0" ]; then
                shiftStage $i
                createStage $i
            fi
            i=$(($i-3))
        done

        checkTimestamp 0
        if [ "$?" == "0" ]; then
            shiftInit
            createInit
        fi

        echo "Finished backup process."
        retval=0
    fi

    echo

    exit $retval
}

main

################################################################################
################################################################################

