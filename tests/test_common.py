import os
from unittest import TestCase

from ebi_eva_common_pyutils.common_utils import merge_two_dicts, pretty_print


class TestCommon(TestCase):

    resources_folder = os.path.join(os.path.dirname(__file__), 'resources')


class TestCommonUtils(TestCase):

    def test_merge_two_dicts(self):
        d1 = {'a': 1, 'b': 2, 'c': 3}
        d2 = {'d': 4, 'a': 5, 'e': 6}
        assert merge_two_dicts(d1, d2) == {'a': 5, 'b': 2, 'c': 3, 'd': 4, 'e': 6}
        assert merge_two_dicts(d2, d1) == {'a': 1, 'b': 2, 'c': 3, 'd': 4, 'e': 6}

    def test_pretty_print(self):
        pretty_print(['Header 1', 'Long Header 2'],
                     [['row1 cell 1', 'row1 cell 2'], ['row2 cell 1', 'Super long row2 cell 2']])
