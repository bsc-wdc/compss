#!/bin/bash

sleep 15

touch /tmp/JavaGAT-test-exists-file
mkdir /tmp/JavaGAT-test-exists-dir
touch ~/JavaGAT-test-exists-file
mkdir ~/JavaGAT-test-exists-dir

touch /tmp/JavaGAT-test-delete-file
mkdir /tmp/JavaGAT-test-delete-dir
touch ~/JavaGAT-test-delete-file
mkdir ~/JavaGAT-test-delete-dir

touch ~/JavaGAT-test-filedir-file
mkdir ~/JavaGAT-test-filedir-dir

touch ~/JavaGAT-test-mode-readable
chmod +r ~/JavaGAT-test-mode-readable
touch ~/JavaGAT-test-mode-unreadable
chmod -r ~/JavaGAT-test-mode-unreadable

touch ~/JavaGAT-test-mode-writable
chmod +w ~/JavaGAT-test-mode-writable
touch ~/JavaGAT-test-mode-unwritable
chmod -w ~/JavaGAT-test-mode-unwritable

echo "hallo" > ~/JavaGAT-test-length

mkdir ~/JavaGAT-test-list
touch ~/JavaGAT-test-list/file1
touch ~/JavaGAT-test-list/file2
mkdir ~/JavaGAT-test-list/dir1

touch ~/JavaGAT-test-last-modified
touch -m -t 198407100000 ~/JavaGAT-test-last-modified

echo s > ~/JavaGAT-test-copy-small

echo l > ~/JavaGAT-test-copy-large
for i in $(seq 1 10); do cat ~/JavaGAT-test-copy-large ~/JavaGAT-test-copy-large >> ~/JavaGAT-test-copy-large_tmp; cat ~/JavaGAT-test-copy-large_tmp ~/JavaGAT-test-copy-large_tmp >> ~/JavaGAT-test-copy-large; done
rm -rf ~/JavaGAT-test-copy-large_tmp

echo s > /tmp/JavaGAT-test-copy-small

echo l > /tmp/JavaGAT-test-copy-large
for i in $(seq 1 10); do cat /tmp/JavaGAT-test-copy-large /tmp/JavaGAT-test-copy-large >> /tmp/JavaGAT-test-copy-large_tmp; cat /tmp/JavaGAT-test-copy-large_tmp /tmp/JavaGAT-test-copy-large_tmp >> /tmp/JavaGAT-test-copy-large; done
rm -rf /tmp/JavaGAT-test-copy-large_tmp

sleep 1


