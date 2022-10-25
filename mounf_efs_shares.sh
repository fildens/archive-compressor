#!/bin/bash

ms=$1
#ms="ARCHIVE"

mkdir -p /mnt/${ms}
/usr/bin/efs-client --host=10.2.0.1 --user=robot --password=robot123 --subfolder=/Unmanaged/${ms}_1/Content --volume-name=ARCHIVE /mnt/${ms}

sleep 5

exit 0
