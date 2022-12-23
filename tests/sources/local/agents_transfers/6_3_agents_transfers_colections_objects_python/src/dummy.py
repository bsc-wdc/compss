"""
This file contains the auxiliary test classes and methods.
"""


class Dummy:
    """Dummy class that represents a generic object."""

    def __init__(self, value: int = 0):
        """Instantiate a new Dummy object.

        :param value: Initial value.
        """
        self.value = value

    def get(self) -> int:
        """Retrieve the value.

        :return: The value.
        """
        return self.value

    def set(self, value):
        """Set the value.

        :param value: New value.
        :return: None.
        """
        self.value = value
