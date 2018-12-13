#!/bin/bash -e

/usr/sbin/sshd -D &
exec "$@"

