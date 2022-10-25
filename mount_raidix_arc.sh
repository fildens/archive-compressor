#!/bin/bash

mkdir -p /mnt/ARC
/sbin/mount.cifs -o user=raidix,pass=Raidix123,file_mode=0644,dir_mode=0755,iocharset=utf8,vers=2.0 //192.168.10.102/ARC /mnt/ARC
