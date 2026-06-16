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
"""Utility helpers for the profiling CLI."""

import sys
from contextlib import nullcontext

try:
    from rich.console import Console
    console = Console()
except ImportError:
    class _FallbackConsole:
        """Minimal fallback class when rich is not installed."""

        # method for stripping rich markup from text, used in the print method
        @staticmethod
        def _strip_markup(text):
            import re
            return re.sub(r"\[/?[^\]]+\]", "", text)

        # Override print to strip rich markup and print plain text
        def print(self, *args, **kwargs):
            text = " ".join(str(a) for a in args)
            print(self._strip_markup(text))

        # Provide a nullcontext for status context manager
        def status(self, msg, **kwargs):
            return nullcontext()

    console = _FallbackConsole()


def _abort():
    """Clean exit on user cancellation (Ctrl-C or None answer)."""
    console.print("\n[bold yellow]⚠  Aborted by user.[/bold yellow]")
    sys.exit(0)


def _ask(prompt_fn):
    """Call a questionary prompt function and abort cleanly if cancelled."""
    result = prompt_fn()
    if result is None:
        _abort()
    return result
