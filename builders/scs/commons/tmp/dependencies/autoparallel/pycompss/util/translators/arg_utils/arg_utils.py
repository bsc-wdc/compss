#!/usr/bin/python

# -*- coding: utf-8 -*-

# For better print formatting
from __future__ import print_function

# Imports
import unittest
import logging

#
# Logger definition
#

logger = logging.getLogger(__name__)


#
# ArgUtils class
#

class ArgUtils(object):

    def __init__(self):
        """
        Initialize internal ArgUtils structures
        """

        self.out_flat_args = None

    def flatten(self, in_vars_len, *args):
        """
        Flats all the arguments store in star_args. The flatten version of the arguments marked
        as IN is returned and the flat version of the arguments marked as OUT is stored internally.

        :param in_vars_len: Number of IN vars on *args
        :param args: List of arguments to flat
        :return: Tuple<List, Int> : A list containing the flattened IN vars and the size of the OUT flat vars
        """

        in_vars = args[:in_vars_len]
        out_vars = args[in_vars_len:]

        if __debug__:
            logger.debug("Flattening input args")
            # logger.debug(in_vars)

        in_flat_args = ArgUtils.flatten_args(*in_vars)

        if __debug__:
            logger.debug("Storing flattened output args")
            # logger.debug(out_vars)

        self.out_flat_args = ArgUtils.flatten_args(*out_vars)
        out_len = len(self.out_flat_args)

        # Return in_args flattened
        return in_flat_args, out_len

    def rebuild(self, out_args):
        """
        Rebuilds the given flatten list into nested objects considering the structure of the
        previously flattened objects

        :param out_args: Flat list of new objects
        :return: List of complex objects with the same structure than the objects in the last flatten call
        """

        if __debug__:
            logger.debug("Rebuilding Args with FO")

        # Check FO list has same size as the original arguments
        assert len(self.out_flat_args) == len(out_args)

        return ArgUtils.rebuild_args(out_args, original_args=self.out_flat_args)

    @staticmethod
    def flatten_args(*args):
        """
        Flats the given arguments

        :param args: List of arguments to flat
        :return: Flattened list
        """

        if __debug__:
            logger.debug("Flattening Args")

        func_args = []
        for arg in args:
            if isinstance(arg, list) or isinstance(arg, tuple):
                s = _Dimension(len(arg))
                flat_arg = ArgUtils._flatten_list(arg)

                func_args.append(s)
                func_args.extend(flat_arg)
            else:
                s = _Dimension()

                func_args.append(s)
                func_args.append(arg)

        if __debug__:
            logger.debug("Flatten result")

        return func_args

    @staticmethod
    def rebuild_args(args, original_args=None):
        """
        Rebuilds the given flatten list into nested objects

        :param args: Flat list of new objects
        :param original_args: Optional list of original structure
        :return: List of complex objects
        """

        if __debug__:
            logger.debug("Rebuilding Args")

        index = 0
        new_args = []
        while index < len(args):
            arg, index = ArgUtils._rebuild_list(args, index, original_args=original_args)
            new_args.append(arg)

        if __debug__:
            logger.debug("Rebuild result")

        return new_args

    @staticmethod
    def _flatten_list(list_to_flatten):
        for elem in list_to_flatten:
            if isinstance(elem, list) or isinstance(elem, tuple):
                yield _Dimension(len(elem))
                for x in ArgUtils._flatten_list(elem):
                    yield x
            else:
                yield elem

    @staticmethod
    def _rebuild_list(args, index, original_args=None):
        sublist_dim = None
        if original_args is None:
            if isinstance(args[index], _Dimension):
                sublist_dim = args[index].get_dimension()
                index = index + 1
        else:
            if isinstance(original_args[index], _Dimension):
                sublist_dim = original_args[index].get_dimension()
                index = index + 1

        if sublist_dim is not None:
            list_object = []
            for _ in range(sublist_dim):
                elem, index = ArgUtils._rebuild_list(args, index, original_args=original_args)
                list_object.append(elem)
            return list_object, index

        # Regular entry
        return args[index], index + 1


#
# Dimension class
#

class _Dimension(object):
    """
    Creates a dimension object to store the size of a nested list

    Attributes:
            - dimension : Size of the nested list
    """

    def __init__(self, dimension=1):
        """
        Creates a Dimension object

        :param dimension: Dimension of the nested object
        """

        self.dimension = dimension

    def get_dimension(self):
        """
        Returns the dimension of the nested object

        :return: The dimension of the nested object
        """
        return self.dimension

    def __eq__(self, other):
        """
        Returns whether the implicit object and the other represent the same dimension value or not

        :param other: Another object to compare with
        :return: True if self and other represent the same value, False otherwise
        """
        if isinstance(other, _Dimension):
            return self.dimension == other.dimension
        else:
            return False

    def __ne__(self, other):
        """
        Returns whether the implicit object and the other represent different dimension values or not

        :param other: Another object to compare with
        :return: True if self and other represent different values, False otherwise
        """

        if isinstance(other, _Dimension):
            return self.dimension != other.dimension
        else:
            return True

    def __str__(self):
        """
        String representation of the Dimension Class

        :return: A string representing the Dimension Class attributes
        """

        return "Dimension(" + str(self.dimension) + ")"

    def __repr__(self):
        """
        String representation of the Dimension Class

        :return: A string representing the Dimension Class attributes
        """

        return self.__str__()


#
# UNIT TEST CASES
#

class TestDimension(unittest.TestCase):

    def test_dimension(self):
        # Create dimension objects
        d1 = _Dimension(dimension=1)
        d2 = _Dimension(dimension=2)
        d3 = _Dimension(dimension=2)

        # Check values
        self.assertEqual(d1.get_dimension(), 1)
        self.assertEqual(d2.get_dimension(), 2)
        self.assertEqual(d3.get_dimension(), 2)

        # Check equality
        self.assertTrue(d1 != d2)
        self.assertFalse(d1 == d2)

        self.assertTrue(d2 == d3)
        self.assertFalse(d2 != d3)

    def test_flatten_1d(self):
        # Create 1D object
        import random

        size = 10
        l1 = [random.random() for _ in range(size)]

        # Flatten arguments
        args_obtained = ArgUtils.flatten_args(l1)

        # Check result
        args_expected = [_Dimension(size)] + l1
        self.assertEqual(args_expected, args_obtained)

    def test_flatten_2d(self):
        # Create 1D object
        import random

        size = 10
        l1 = [[random.random() for _ in range(size)] for _ in range(size)]

        # Flatten arguments
        args_obtained = ArgUtils.flatten_args(l1)

        # Check result
        args_expected = [_Dimension(size)]
        for sub_l in l1:
            args_expected.append(_Dimension(size))
            args_expected.extend(sub_l)
        self.assertEqual(args_expected, args_obtained)

    def test_flatten_multi_1d(self):
        # Create 1D object
        import random

        size1 = 10
        size2 = 5
        l1 = [random.random() for _ in range(size1)]
        l2 = [random.random() for _ in range(size2)]

        # Flatten arguments
        args_obtained = ArgUtils.flatten_args(l1, l2)

        # Check result
        args_expected = [_Dimension(size1)] + l1 + [_Dimension(size2)] + l2
        self.assertEqual(args_expected, args_obtained)

    def test_flatten_multi(self):
        # Create 1D object
        import random

        size1 = 10
        size2 = 5
        l1 = [random.random() for _ in range(size1)]
        l2 = [[random.random() for _ in range(size2)] for _ in range(size2)]

        # Flatten arguments
        args_obtained = ArgUtils.flatten_args(l1, l2)

        # Check result
        args_expected = [_Dimension(size1)] + l1 + [_Dimension(size2)]
        for sub_l in l2:
            args_expected.append(_Dimension(size2))
            args_expected.extend(sub_l)
        self.assertEqual(args_expected, args_obtained)

    def test_rebuild_1d(self):
        # Create 1D object
        import random

        size = 10
        l1 = [random.random() for _ in range(size)]

        func_args = [_Dimension(size)] + l1

        # Rebuild arguments
        args_obtained = ArgUtils.rebuild_args(func_args)

        # Check result
        args_expected = [l1]
        self.assertEqual(args_expected, args_obtained)

    def test_rebuild_2d(self):
        # Create 1D object
        import random

        size = 10
        l1 = [[random.random() for _ in range(size)] for _ in range(size)]

        func_args = [_Dimension(size)]
        for sub_l in l1:
            func_args.append(_Dimension(size))
            func_args.extend(sub_l)

        # Rebuild arguments
        args_obtained = ArgUtils.rebuild_args(func_args)

        # Check result
        args_expected = [l1]
        self.assertEqual(args_expected, args_obtained)

    def test_rebuild_multi_1d(self):
        # Create 1D object
        import random

        size1 = 10
        size2 = 5
        l1 = [random.random() for _ in range(size1)]
        l2 = [random.random() for _ in range(size2)]

        func_args = [_Dimension(size1)] + l1 + [_Dimension(size2)] + l2

        # Rebuild arguments
        args_obtained = ArgUtils.rebuild_args(func_args)

        # Check result
        args_expected = [l1, l2]
        self.assertEqual(args_expected, args_obtained)

    def test_rebuild_multi(self):
        # Create 1D object
        import random

        size1 = 10
        size2 = 5
        l1 = [random.random() for _ in range(size1)]
        l2 = [[random.random() for _ in range(size2)] for _ in range(size2)]

        func_args = [_Dimension(size1)] + l1 + [_Dimension(size2)]
        for sub_l in l2:
            func_args.append(_Dimension(size2))
            func_args.extend(sub_l)

        # Rebuild arguments
        args_obtained = ArgUtils.rebuild_args(func_args)

        # Check result
        args_expected = [l1, l2]
        self.assertEqual(args_expected, args_obtained)


#
# MAIN FOR UNIT TEST
#

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s | %(levelname)s | %(name)s - %(message)s')
    unittest.main()
