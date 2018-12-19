#!/bin/bash -e

sudo /usr/sbin/sshd -D &
exec "$@"

