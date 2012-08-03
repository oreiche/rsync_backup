#!/bin/bash

g_backupdir="/backup/"
g_sourcedir="/"
g_excludedirs=("dev" "mnt" "tmp")

g_default="day"
g_snapshots="6"

g_stages=(
    #<name> <condition>          <snapshots>
    "week"  "$(date +%w) == 0"    "4"
    "month" "$(date +%d) == 01"  "11"
    "year"  "$(date +%j) == 001"  "3"
)

function createStage() {
    local curr=${g_stages[$1]}
    local prev
    local last

    if [ $1 -ge 3 ]; then
        prev=${g_stages[$(($1-3))]}
    else
        prev=$g_default
    fi
    last="$(ls $g_backupdir/ | grep $prev | sort -n -t '.' -k 2 | tail -n1)"

    if [ "$last" != "" ] && 
       [ "$last" != $prev.0 ]; then
        echo "Creating new snapshot for stage '$curr'."
        rm -rf $g_backupdir/$curr.1
        mv $g_backupdir/$last $g_backupdir/$curr.1 2>/dev/null
    fi
}

function shiftStage() {
    local curr=${g_stages[$1]}
    local i=${g_stages[$(($1+2))]}

    echo "Shifting stage '$curr'."

    rm -rf $g_backupdir/$curr.$i
    while [ $i -gt 1 ]; do
        mv $g_backupdir/$curr.$(($i-1)) $g_backupdir/$curr.$i 2>/dev/null
        i=$(($i-1))
    done
}

function createDefault() {
    echo "Creating new snapshot for default stage '$g_default'."

    if [ -d $g_backupdir/$g_default.tmp ]; then
        # Reuse the temp directory
        mv $g_backupdir/$g_default.0 $g_backupdir/$g_default.1
        mv $g_backupdir/$g_default.tmp $g_backupdir/$g_default.0
        if [[ $OSTYPE == *darwin* ]]; then
            cd $g_backupdir/$g_default.1
            find . -print | cpio -pdlm $g_backupdir/$g_default.0 2>/dev/null
        else
            cp -al $g_backupdir/$g_default.1/. $g_backupdir/$g_default.0
        fi
    elif [ -d $g_backupdir/$g_default.0 ]; then
        rm -rf $g_backupdir/$g_default.1
        if [[ $OSTYPE == *darwin* ]]; then
            cd $g_backupdir/$g_default.0
            find . -print | cpio -pdlm $g_backupdir/$g_default.1 2>/dev/null
        else
            cp -al $g_backupdir/$g_default.0 $g_backupdir/$g_default.1
        fi
    fi

    local excluded
    local exclude=$(echo $g_backupdir | grep $g_sourcedir | \
                    sed "s/^$(echo $g_sourcedir | sed 's/\//\\\//g')\///g")
    if [ "$exclude" != "" ]; then
        excluded="$excluded --exclude=$exclude"
    fi
    for exclude in "${g_excludedirs[@]}"; do
        excluded="$excluded --exclude=$exclude"
    done

    rsync -a --delete $excluded $g_sourcedir/ $g_backupdir/$g_default.0/
}

function shiftDefault() {
    local i=$g_snapshots

    echo "Shifting default stage '$g_default'."

    if [ -d $g_backupdir/$g_default.$i ]; then
        # Store as temp, this speeds up the whole operation
        mv $g_backupdir/$g_default.$i $g_backupdir/$g_default.tmp
    fi

    while [ $i -gt 1 ]; do
        mv $g_backupdir/$g_default.$(($i-1)) $g_backupdir/$g_default.$i 1>/dev/null
        i=$(($i-1))
    done
}

function main() {
    local n=${#g_stages[*]}

    if [ $(($n % 3)) -ne 0 ]; then
        echo "Configuration error: Malformed stages array."
        exit 1
    elif [ ! -d $g_sourcedir ]; then
        echo "Directory '$g_sourcedir' does not exist."
        exit 1
    elif [ ! -d $g_backupdir ]; then
        echo "Directory '$g_backupdir' does not exist."
        exit 1
    else
        local i=$(($n - 3))
        while [ $i -ge 0 ]; do
            if [ ${g_stages[$(($i+1))]} ]; then
                shiftStage $i
                createStage $i
            fi
            i=$(($i-3))
        done

        shiftDefault
        createDefault
    fi

    exit 0
}

main

