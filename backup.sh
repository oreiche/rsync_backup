#!/bin/bash
#
# (C) Oliver Reiche <oliver.reiche@siemens.com>

conf_sourcedir="/"
conf_backupdir="/backup"
conf_excludedirs=("dev" "mnt" "tmp")

conf_name="day"
conf_snapshots="6"

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
    local timestamp=$(cat $conf_backupdir/$curr.stamp 2>/dev/null)
    local retval=1

    if [ "$timestamp" == "" ]; then
        echo $g_timestamp > $conf_backupdir/$curr.stamp
    elif [ $((($(date +%s) - $timestamp) / 60)) -ge $minutes ]; then
        echo $g_timestamp > $conf_backupdir/$curr.stamp
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
    last="$(ls $conf_backupdir/ | grep $prev\.[[:digit:]] | \
            sort -n -t '.' -k 2 | tail -n1)"

    if [ "$last" != "" ] && 
       [ "$last" != $prev.0 ]; then
        echo "Creating new snapshot for stage '$curr'."
        rm -rf $conf_backupdir/$curr.1
        mv $conf_backupdir/$last $conf_backupdir/$curr.1 2>/dev/null
    fi
}

################################################################################

function shiftStage() {
    local curr=${conf_stages[$1]}
    local i=${conf_stages[$(($1+2))]}

    if [ -d $conf_backupdir/$curr.1 ]; then
        echo "Shifting snapshots of stage '$curr'."
        rm -rf $conf_backupdir/$curr.$i
        while [ $i -gt 1 ]; do
            mv $conf_backupdir/$curr.$(($i-1)) \
                $conf_backupdir/$curr.$i 2>/dev/null
            i=$(($i-1))
        done
    fi
}

################################################################################

function createInit() {
    echo "Creating new snapshot for initial stage '$conf_name'."

    if [ -d $conf_backupdir/$conf_name.tmp ]; then
        # Reuse the temp directory
        mv $conf_backupdir/$conf_name.0 $conf_backupdir/$conf_name.1
        mv $conf_backupdir/$conf_name.tmp $conf_backupdir/$conf_name.0
        if [[ $OSTYPE == *darwin* ]]; then
            cd $conf_backupdir/$conf_name.1
            find . -print | cpio -pdlm $conf_backupdir/$conf_name.0 2>/dev/null
        else
            cp -al $conf_backupdir/$conf_name.1/. $conf_backupdir/$conf_name.0
        fi
    elif [ -d $conf_backupdir/$conf_name.0 ]; then
        rm -rf $conf_backupdir/$conf_name.1
        if [[ $OSTYPE == *darwin* ]]; then
            cd $conf_backupdir/$conf_name.0
            find . -print | cpio -pdlm $conf_backupdir/$conf_name.1 2>/dev/null
        else
            cp -al $conf_backupdir/$conf_name.0 $conf_backupdir/$conf_name.1
        fi
    fi

    local excluded
    local exclude=$(echo $conf_backupdir | grep $conf_sourcedir | \
                    sed "s/^$(echo $conf_sourcedir | sed 's/\//\\\//g')\///g")
    if [ "$exclude" != "" ]; then
        excluded="$excluded --exclude=$exclude"
    fi
    for exclude in "${conf_excludedirs[@]}"; do
        excluded="$excluded --exclude=$exclude"
    done

    rsync -a --delete $excluded $conf_sourcedir/ $conf_backupdir/$conf_name.0/
}

################################################################################

function shiftInit() {
    local i=$conf_snapshots

    echo "Shifting snapshots of initial stage '$conf_name'."

    if [ -d $conf_backupdir/$conf_name.$i ]; then
        # Store as temp, this speeds up the whole operation
        mv $conf_backupdir/$conf_name.$i $conf_backupdir/$conf_name.tmp
    fi

    while [ $i -gt 1 ]; do
        mv $conf_backupdir/$conf_name.$(($i-1)) $conf_backupdir/$conf_name.$i 2>/dev/null
        i=$(($i-1))
    done
}

################################################################################

function main() {
    local n=${#conf_stages[*]}

    if [ $(($n % 3)) -ne 0 ]; then
        echo "Configuration error: Malformed stages array."
        exit 1
    elif [ ! -d $conf_sourcedir ]; then
        echo "Directory '$conf_sourcedir' does not exist."
        exit 1
    elif [ ! -d $conf_backupdir ]; then
        echo "Directory '$conf_backupdir' does not exist."
        exit 1
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
    fi

    exit 0
}

main

################################################################################
################################################################################

