#!/bin/bash

echo "abcdefghijklmnopqrstuvwxyz" > ~/JavaGAT-test-fileinputstream
for i in $(seq 1 10); do cat ~/JavaGAT-test-fileinputstream ~/JavaGAT-test-fileinputstream >> ~/JavaGAT-test-fileinputstream_tmp; cat ~/JavaGAT-test-fileinputstream_tmp ~/JavaGAT-test-fileinputstream_tmp >> ~/JavaGAT-test-fileinputstream; done
rm -rf ~/JavaGAT-test-fileinputstream_tmp