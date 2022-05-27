import os
from unittest import TestCase

from ebi_eva_common_pyutils.common_utils import merge_two_dicts


class TestCommon(TestCase):

    resources_folder = os.path.join(os.path.dirname(__file__), 'resources')


class TestCommonUtils(TestCase):

    def test_merge_two_dicts(self):
        d1 = {'a': 1, 'b': 2, 'c': 3}
        d2 = {'d': 4, 'a': 5, 'e': 6}
        assert merge_two_dicts(d1, d2) == {'a': 5, 'b': 2, 'c': 3, 'd': 4, 'e': 6}
        assert merge_two_dicts(d2, d1) == {'a': 1, 'b': 2, 'c': 3, 'd': 4, 'e': 6}
