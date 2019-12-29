#!/bin/bash
#
# (c) 2018, Oliver Reiche <oliver.reiche@gmail.com>

# Stages for storing snapshots (ascending interval length)
conf_stages=(
    #NAME       MINUTES  COUNT
    "day"        "1440"    "6"
    "week"      "10080"    "3"
    "month"     "40320"    "2"
    "quarter"  "120960"    "1"
    "halfyear" "241920"    "1"
    "year"     "483840"    "3"
)

################################################################################

## Time stamp of initial script execution (@see checkTimestamp())
g_timestamp=$(date +%s)

################################################################################

##
## @brief Print usage information.
##
function printUsage() {
    echo "Usage:"
    echo "  $0 <sourcepath> <backuppath> [excludepath1 excludepath2 ...]"
    echo
    echo "Example: Backup '/' to '/backup' without '/dev' '/mnt' '/tmp'"
    echo "  $0 / /backup dev mnt tmp"
}

################################################################################

##
## @brief Resets an existing time stamp.
## @param {Number} $1 Index of the stage in stages array.
##
function resetTimestamp() {
    local curr=${conf_stages[$1]}
    local seconds=$((${conf_stages[$(($1+1))]} * 60))
    local timestamp=$(cat "$conf_backuppath"/.$curr.stamp 2>/dev/null)

    if [ "$timestamp" != "" ]; then
        # Floor new time stamp to multiple of $seconds
        echo $(($g_timestamp / $seconds * $seconds)) \
            > "$conf_backuppath"/.$curr.stamp
    fi
}

################################################################################

##
## @brief Checks whether time stamp of stage is exceeded. Creates new time stamp
##        if it doesn't already exist or resets an existing time stamp.
## @param  {Number} $1  Index of the stage in stages array.
## @retval {String} "0" Time stamp was exceeded or didn't exist.
## @retval {String} "1" Time stamp was not exceeded.
##
function checkTimestamp() {
    local curr=${conf_stages[$1]}
    local seconds=$((${conf_stages[$(($1+1))]} * 60))
    local timestamp=$(cat "$conf_backuppath"/.$curr.stamp 2>/dev/null)
    local retval=1

    if [ "$timestamp" != "" ]; then
        local delta=$(($(date +%s) - $timestamp))
        if [ $delta -ge $seconds ]; then
            resetTimestamp $1
            retval=0
        fi
    else
        # Creating initial time stamp
        echo $g_timestamp > "$conf_backuppath"/.$curr.stamp
        retval=0
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
    local last="$(ls "$conf_backuppath"/ | grep $prev\.[[:digit:]]*$ | \
                  sort -n -t '.' -k 2 | tail -n1)"

    if [ "$last" != "" ] && 
       [ "$last" != $prev.0 ]; then
        echo "Creating new snapshot for stage '$curr'."
        if [ -d "$conf_backuppath"/$curr.1 ]; then
            mv -f "$conf_backuppath"/$curr.1 "$conf_backuppath"/$curr.del
            rm -rf "$conf_backuppath"/$curr.del 2>/dev/null &
        fi
        mv "$conf_backuppath"/$last "$conf_backuppath"/$curr.1 2>/dev/null
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

    if [ -d "$conf_backuppath"/$curr.1 ]; then
        echo "Shifting snapshots of stage '$curr'."
        if [ -d "$conf_backuppath"/$curr.$i ]; then
            mv -f "$conf_backuppath"/$curr.$i "$conf_backuppath"/$curr.del
            rm -rf "$conf_backuppath"/$curr.del 2>/dev/null &
        fi
        while [ $i -gt 1 ]; do
            mv "$conf_backuppath"/$curr.$(($i-1)) \
               "$conf_backuppath"/$curr.$i 2>/dev/null
            i=$(($i-1))
        done
    fi
}

################################################################################

##
## @brief Creates a new snapshot for initial stage. A previous shifting process
##        might has reserved a temp directory which will be used to speed up the
##        creation process.
## @returns Return value of rsync process call.
##
function createInit() {
    local init=${conf_stages[0]}

    echo "Creating new snapshot for initial stage '$init'."

    if [ -d "$conf_backuppath"/$init.tmp ]; then
        # Reuse the temp directory
        mv "$conf_backuppath"/$init.tmp "$conf_backuppath"/$init.0
    fi

    if [ -d "$conf_backuppath"/$init.1 ]; then
        echo "  Creating hard copy from previous backup." \
            | tee "$conf_backuppath"/backup.log
        if [[ $OSTYPE == *darwin* ]]; then
            cd "$conf_backuppath"/$init.1
            find . -print | cpio -pdlm "$conf_backuppath"/$init.0 \
                &>>"$conf_backuppath"/backup.log
        else
            cp -alu "$conf_backuppath"/$init.1/. "$conf_backuppath"/$init.0 \
                &>>"$conf_backuppath"/backup.log
        fi
    fi

    local excluded=
    local exclude=$(echo "$conf_backuppath" | grep "$conf_sourcepath" | \
                    sed "s/^$(echo "$conf_sourcepath" | \
                        sed 's/\//\\\//g')\/*//g")
    if [ "$exclude" != "" ]; then
        excluded="$excluded --exclude=\"$exclude\""
    fi
    for exclude in "${conf_excludepaths[@]}"; do
        excluded="$excluded --exclude=\"$exclude\""
    done

    # Test for non-cross-device and let rsync create hard links instead
    touch "$conf_sourcepath"/.cross-device_link_test
    mkhardlink=""
    if [[ $OSTYPE == *darwin* ]]; then
        cd "$conf_sourcepath"/
        find .cross-device_link_test -print | \
            cpio -pdlm "$conf_backuppath"/.cross-device_link_test 2>/dev/null
    else
        cp -alu "$conf_sourcepath"/.cross-device_link_test \
            "$conf_backuppath"/.cross-device_link_test 2>/dev/null
    fi
    if [ "$?" == "0" ]; then
      mkhardlink="--link-dest=\"$conf_sourcepath\""
    fi
    rm -f "$conf_sourcepath"/.cross-device_link_test
    rm -f "$conf_backuppath"/.cross-device_link_test

    cmd='rsync -a --delete -h --info=progress2'
    cmd=$cmd' '$mkhardlink
    cmd=$cmd' '$excluded
    cmd=$cmd' "'$conf_sourcepath'"/'
    cmd=$cmd' "'$conf_backuppath'"/'$init'.0/'
    cmd=$cmd' 2>>"'$conf_backuppath'"/backup.log'

    echo "  Running rsync to create the actual backup." \
        | tee -a "$conf_backuppath"/backup.log
    eval $cmd

    local retval=$?
    if [ "$retval" != "0" ]; then
        echo "  WARNING: rysnc returned exit code '$retval'."
        echo "           For more details see '$conf_backuppath/backup.log'."
    fi

    return $retval
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

    if [ -d "$conf_backuppath"/$init.0 ]; then
        echo "Shifting snapshots of initial stage '$init'."

        if [ -d "$conf_backuppath"/$init.$i ]; then
            # Store as temp, this speeds up the whole operation
            mv "$conf_backuppath"/$init.$i "$conf_backuppath"/$init.tmp
        fi

        while [ $i -gt 0 ]; do
            mv "$conf_backuppath"/$init.$(($i-1)) \
               "$conf_backuppath"/$init.$i 2>/dev/null
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

    if [ $1 ] && [ $2 ]; then
        # Parse command line arguments
        conf_sourcepath="$1"
        shift
        conf_backuppath="$1"
        shift
        conf_excludepaths=("$@")

        if [ $n -lt 3 ] ||
           [ $(($n % 3)) -ne 0 ]; then
            echo "Configuration error: Malformed stages array."
        elif [ ! -d "$conf_sourcepath" ]; then
            echo "Directory '$conf_sourcepath' does not exist."
        elif [ ! -d "$conf_backuppath" ]; then
            echo "Directory '$conf_backuppath' does not exist."
        else
            echo "Starting backup of '$conf_sourcepath' to '$conf_backuppath'."

            if [ ! -f "$conf_backuppath"/.inprogress.stamp ]; then
                echo $g_timestamp > "$conf_backuppath"/.inprogress.stamp

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
                    retval=$?
                else
                    retval=0
                fi
            else
                echo "Recovering interrupted snapshot for initial stage."
                rm -rf "$conf_backuppath"/*.del 2>/dev/null &
                resetTimestamp 0
                createInit
                retval=$?
            fi

            rm -f "$conf_backuppath"/.inprogress.stamp
            echo "Finished backup process."
        fi
    else
        printUsage
        retval=0
    fi

    return $retval
}

main "$@"
exit $?

################################################################################
################################################################################

