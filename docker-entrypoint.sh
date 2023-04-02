#!/bin/bash -e

SWAP_SIZE=$(($(stat -f -c "(%a*%s/10)*7" .)))

fallocate -l $SWAP_SIZE /swapfile

chmod 0600 /swapfile

mkswap /swapfile
echo 10 > /proc/sys/vm/swappiness
swapon /swapfile
echo 1 > /proc/sys/vm/overcommit_memory

python start_updater.py