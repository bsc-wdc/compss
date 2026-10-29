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
"""
Redaction of secrets (passwords, tokens, keys...) from the values recorded in the RO-Crate.

The crate can be published (WorkflowHub, Zenodo, ROHub), so the environment variables and the submission
command recorded in it must not contain secrets.
"""

import re
import typing

REDACTED = "***REDACTED***"

# Words that mark a name as sensitive, compared with each part of the name (split by '_', '.', '-' and camelCase)
SENSITIVE_PARTS = {
    "PASSWORD",
    "PASSWD",
    "PASS",
    "PASSPHRASE",
    "SECRET",
    "TOKEN",
    "APIKEY",
    "CREDENTIAL",
    "CREDENTIALS",
    "AUTH",
    "OAUTH",
}
# Pairs of consecutive parts that mark a name as sensitive (e.g. API_KEY, PRIVATE_KEY)
SENSITIVE_PAIRS = {("API", "KEY"), ("PRIVATE", "KEY"), ("ACCESS", "KEY")}

# scheme://user:password@host
URL_CREDENTIALS = re.compile(r"(?P<pre>[A-Za-z][A-Za-z0-9+.-]*://[^\s:/@]+:)[^\s@/]+@")
# NAME=VALUE, --name=VALUE, -name=VALUE, -Dname=VALUE (the value can be quoted)
ASSIGNMENT = re.compile(r"(?P<pre>(?<![^\s\"'])-{0,2}(?P<name>[A-Za-z_][\w.\-]*)=)(?P<value>\"[^\"]*\"|'[^']*'|[^\s\"']+)")
# --name VALUE, -name VALUE (the value cannot start with '-')
OPTION_VALUE = re.compile(r"(?P<pre>(?<!\S)-{1,2}(?P<name>[A-Za-z][\w.\-]*)\s+)(?P<value>\"[^\"]*\"|'[^']*'|[^\s\"'\-]\S*)")


def _name_parts(name: str) -> typing.List[str]:
    """
    Split a name into upper case parts, by '_', '.', '-' and camelCase boundaries

    :param name: Name of a variable, option or property (e.g. javax.net.ssl.keyStorePassword)

    :returns: List of upper case parts (e.g. ['JAVAX', 'NET', 'SSL', 'KEY', 'STORE', 'PASSWORD'])
    """

    camel_split = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", name)
    return [part.upper() for part in re.split(r"[^A-Za-z0-9]+", camel_split) if part]


def is_sensitive_name(name: str) -> bool:
    """
    Check if a name (environment variable, option or property) usually holds a secret

    :param name: Name to check

    :returns: True if the name contains a sensitive word (e.g. COMPSS_DB_TOKEN, --api-key, -Dx.password)
    """

    parts = _name_parts(name)
    if any(part in SENSITIVE_PARTS for part in parts):
        return True
    return any(pair in SENSITIVE_PAIRS for pair in zip(parts, parts[1:]))


def redact_text(text: str) -> typing.Tuple[str, typing.List[str]]:
    """
    Mask the secrets found inside a value or a command line

    Masks the password of URLs with credentials, and the values of NAME=VALUE pairs and options (--name VALUE,
    --name=VALUE, -Dname=VALUE) whose name is sensitive.

    :param text: Value or command line

    :returns: The text with the secrets masked, and the names of what was masked
    """

    found = []

    def mask_url(match: re.Match) -> str:
        found.append("URL credentials")
        return match.group("pre") + REDACTED + "@"

    def mask_value(match: re.Match) -> str:
        if not is_sensitive_name(match.group("name")):
            # The value can hold other secrets (e.g. JAVA_OPTS=-Dx.password=VALUE)
            inner_value, inner_found = redact_text(match.group("value"))
            found.extend(inner_found)
            return match.group("pre") + inner_value
        found.append(match.group("name"))
        return match.group("pre") + REDACTED

    text = URL_CREDENTIALS.sub(mask_url, text)
    text = ASSIGNMENT.sub(mask_value, text)
    text = OPTION_VALUE.sub(mask_value, text)
    return text, found


def redact_variable(name: str, value: str) -> typing.Tuple[str, bool]:
    """
    Redact the value of an environment variable

    :param name: Name of the variable
    :param value: Value of the variable

    :returns: The value to record, and whether something was masked
    """

    if is_sensitive_name(name):
        return REDACTED, True
    new_value, found = redact_text(value)
    return new_value, len(found) > 0
