#!/bin/bash

ms=$1
#ms="ARCHIVE"

DIR="${BASH_SOURCE%/*}"
export $(grep -v '^#' "${DIR}/.env" | xargs)

mkdir -p /mnt/"$ms"
/usr/bin/efs-client --host=$ES_HOST --user=$ES_USER --password=$ES_PASS --subfolder=/Unmanaged/"$ms"_1/Content --volume-name=ARCHIVE /mnt/"$ms"

exit 0
