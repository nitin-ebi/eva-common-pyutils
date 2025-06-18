import os
from os.path import join

import pytest

from ebi_eva_common_pyutils.config import Configuration, WritableConfig
from tests.test_common import TestCommon


class TestConfiguration(TestCommon):
    def setUp(self):
        self.config_file = os.path.join(self.resources_folder, 'test_config.yml')
        self.cfg = Configuration(self.config_file)

    def test_delayed_load(self):
        cfg2 = Configuration()
        assert cfg2.get('ftp_dir') is None
        cfg2.load_config_file(self.config_file)
        assert cfg2.get('ftp_dir') == '/path/to/ftp/dir'

    def test_load_config_file(self):
        non_existing_cfg_file = join(self.resources_folder, 'non_existent.yaml')
        with pytest.raises(FileNotFoundError) as e:
            self.cfg.load_config_file(non_existing_cfg_file)
            assert str(e) == 'Could not find any config file in specified search path'

    def test_query(self):
        assert self.cfg.query('ftp_dir') == '/path/to/ftp/dir'
        assert self.cfg.query('nonexistent_thing') is None
        assert self.cfg.query('submission', 'metadata_spreadsheet') == '/path/to/metadata.xlsx'

    def test_contains(self):
        assert 'ftp_dir' in self.cfg

class TestWritableConfiguration(TestCommon):

    def setUp(self):
        self.config_file = os.path.join(self.resources_folder, 'new_config.yml')
        self.cfg = WritableConfig(self.config_file)

    def tearDown(self):
        if os.path.exists(self.config_file):
            os.remove(self.config_file)

    def test_set(self):
        self.cfg.set('path1', value='value1')
        self.cfg.set('path2', 'subpath1', value='value1')
        assert self.cfg.query('path1') == 'value1'
        assert self.cfg.query('path2', 'subpath1') == 'value1'


    def test_set_overwrite_new_dict(self):
        self.cfg.set('path1', value='value1')
        assert self.cfg.query('path1') == 'value1'
        self.cfg.set('path1', value={'key1': 'value1'})
        assert self.cfg.query('path1') == {'key1': 'value1'}

        self.cfg.set('path2', value='value1')
        assert self.cfg.query('path2') == 'value1'
        # Overwrite the existing key value pair path2:value1 with new dict
        self.cfg.set('path2','subpath1', value='value1')
        assert self.cfg.query('path2', 'subpath1') == 'value1'
