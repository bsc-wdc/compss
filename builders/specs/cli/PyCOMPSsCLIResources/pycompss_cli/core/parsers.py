#!/usr/bin/env python3
#
#  Copyright 2002-2026 Barcelona Supercomputing Center (www.bsc.es)
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
import argparse
import re


class TaskIDAction(argparse.Action):
    """
    Parse tokens like:
      5
      3-7
      1,2,4-6
      [6-10]
    into a single flat list of ints stored in namespace.
    Raises argparse.ArgumentError for bad tokens/ranges so argparse prints a friendly error.
    """

    def __call__(self, parser, namespace, values, option_string=None):
        existing = getattr(namespace, self.dest, None)
        if existing is None:
            existing = []

        # values can be a list (nargs='+') or a single string
        tokens = values if isinstance(values, (list, tuple)) else [values]

        for token in tokens:
            # split by commas or whitespace (so "1,2 4-6" works)
            for part in re.split(r'[,\s]+', token):
                if not part:
                    continue
                p = part.strip()

                # accept bracketed forms like "[6-10]"
                if p.startswith('[') and p.endswith(']'):
                    p = p[1:-1].strip()

                # single integer
                if re.fullmatch(r'\d+', p):
                    existing.append(p)
                    continue

                # range "start-end"
                m = re.fullmatch(r'(\d+)-(\d+)', p)
                if m:
                    start = int(m.group(1))
                    end = int(m.group(2))
                    if start > end:
                        raise argparse.ArgumentError(self,
                                                     f"Invalid range '{part}': start must be <= end")
                    # inclusive range
                    existing.extend(str(elem) for elem in range(start, end + 1))
                    continue

                # anything else is invalid
                raise argparse.ArgumentError(self,
                                             f"Invalid value '{part}': expected integer or range like 5-10")

        # remove duplicates while preserving order (optional; keeps first-seen order)
        existing = list(dict.fromkeys(existing))

        setattr(namespace, self.dest, existing)
