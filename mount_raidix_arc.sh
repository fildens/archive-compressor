#!/bin/bash

# ms=$1
ms="ARC"

DIR="${BASH_SOURCE%/*}"
export $(grep -v '^#' "${DIR}/.env" | xargs)

mkdir -p /mnt/${ms}
/sbin/mount.cifs -o user=$R_USER,pass=$R_PASS,file_mode=0644,dir_mode=0755,iocharset=utf8,vers=2.0 //"$R_HOST"/ARC /mnt/${ms}
