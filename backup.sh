#!/bin/bash

g_backupdir="/backup/"
g_sourcedir="/"
g_excludedirs=("dev" "mnt" "tmp")

g_config=(
    #<name> <condition> <number-of-backups>
    "year"  "$(date +%j) == 001"  "3"
    "month" "$(date +%d) == 01"  "11"
    "week"  "$(date +%w) == 0"    "4"
    "day"   "true"                "6"
)

function createinc() {
    local curr=${g_config[$1]}
    local prev=${g_config[$(($1+3))]}
    local last="$(ls $g_backupdir/ | grep $prev | sort -n -t '.' -k 2 | tail -n1)"

    if [ "$last" != "" ]; then
        rm -rf $g_backupdir/$curr.1
        cp -al $g_backupdir/$last $g_backupdir/$curr.1
    fi
}

function rotateinc() {
    local curr=${g_config[$1]}
    local i=${g_config[$(($1+2))]}

    rm -rf $g_backupdir/$curr.$i
    while [ $i -gt 1 ]; do
        mv $g_backupdir/$curr.$(($i-1)) $g_backupdir/$curr.$i
        i=$(($i-1))
    done
}

function createinit() {
    local curr=${g_config[$1]}

    if [ -d $g_backupdir/$curr.tmp ]; then
        # Reuse the temp directory
        mv $g_backupdir/$curr.0 $g_backupdir/$curr.1
        mv $g_backupdir/$curr.tmp $g_backupdir/$curr.0
        cp -al $g_backupdir/$curr.1/. $g_backupdir/$curr.0
    else
        rm -rf $g_backupdir/$curr.1
        cp -al $g_backupdir/$curr.0 $g_backupdir/$curr.1
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

    rsync -a --delete $excluded $g_sourcedir/ $g_backupdir/$curr.0/
}

function rotateinit() {
    local curr=${g_config[$1]}
    local i=${g_config[$1+2]}

    if [ -d $g_backupdir/$curr.$i ]; then
        # Store as temp, this speeds up the whole operation
        mv $g_backupdir/$curr.$i $g_backupdir/$curr.tmp
    fi

    while [ $i -gt 1 ]; do
        mv $g_backupdir/$curr.$(($i-1)) $g_backupdir/$curr.$i
        i=$(($i-1))
    done
}

function main() {
    local n=${#g_config[*]}

    if [ $(($n % 3)) -ne 0 ] ||
       [ $n -lt 3 ]; then
        echo "Configuration error."
        exit 1
    else
        local i=0
        while [ $(($n-$i)) -ne 3 ]; do
            if [ ${g_config[$(($i+1))]} ]; then
                rotateinc $i
                createinc $i
            fi
            i=$(($i+3))
        done

        if [ ${g_config[$(($i+1))]} ]; then
            rotateinit $i
            createinit $i
        fi
    fi

    exit 0
}

main

