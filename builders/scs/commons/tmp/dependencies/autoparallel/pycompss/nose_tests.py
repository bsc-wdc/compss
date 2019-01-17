#!/usr/bin/python

# -*- coding: utf-8 -*-

# Imports
import nose
import sys
from nose.plugins.base import Plugin


class ExtensionPlugin(Plugin):
    name = "ExtensionPlugin"

    def options(self, parser, env):
        Plugin.options(self, parser, env)

    def configure(self, options, config):
        Plugin.configure(self, options, config)
        self.enabled = True

    @classmethod
    def wantFile(cls, file_name):
        return file_name.endswith('.py')

    @classmethod
    def wantDirectory(cls, directory):
        return not ('tests' in directory)

    @classmethod
    def wantModule(cls, module_name):
        return True


if __name__ == '__main__':
    includeDirs = ["-w", "."]
    nose.main(addplugins=[ExtensionPlugin()],
              argv=sys.argv.extend(includeDirs))
