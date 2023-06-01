# Copyright 2020 EMBL - European Bioinformatics Institute
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import os
from lxml.etree import XPathEvalError

from ebi_eva_common_pyutils.config_utils import EVAPrivateSettingsXMLConfig, get_pg_metadata_uri_for_eva_profile, \
    get_mongo_uri_for_eva_profile, get_mongo_creds_for_profile
from tests.test_common import TestCommon


class TestEVAPrivateSettingsXMLConfig(TestCommon):

    def setUp(self) -> None:
        self.config_file = os.path.join(self.resources_folder, 'test_config_file.xml')
        self.private_settings = EVAPrivateSettingsXMLConfig(self.config_file)

    def test_get_value_with_xpath(self):
        self.assertEqual(
            self.private_settings.get_value_with_xpath(
                '//settings/profiles/profile/id[text()="test"]/../properties/eva.mongo.host/text()'
            ),
            ['mongo.example.com:27017,mongo.example-primary.com:27017']
        )
        self.assertRaises(
            ValueError,
            self.private_settings.get_value_with_xpath,
            '//settings/profiles/profile/id[text()="failedtest"]/../properties/eva.mongo.host/text()'
        )
        self.assertRaises(
            XPathEvalError,
            self.private_settings.get_value_with_xpath,
            '//anystring/{}'
        )


class TestDatabaseConfig(TestCommon):

    def setUp(self) -> None:
        self.config_file = os.path.join(os.path.dirname(__file__), 'resources', 'test_config_file.xml')

    def test_get_pg_metadata_uri_for_eva_profile(self):
        self.assertEqual(
            get_pg_metadata_uri_for_eva_profile('test', self.config_file),
            'postgresql://pgsql.example.com:5432/testdatabase'
        )

    def test_get_mongo_uri_for_eva_profile(self):
        self.assertEqual(
            get_mongo_uri_for_eva_profile('test', self.config_file),
            'mongodb://testuser:testpassword@mongo.example.com:27017,mongo.example-primary.com:27017/admin'
        )
        self.assertRaises(
            ValueError,
            get_mongo_uri_for_eva_profile, 'test1', self.config_file
        )
        # test for local mongo with no authentication
        self.assertEqual(
            get_mongo_uri_for_eva_profile('local', self.config_file),
            'mongodb://localhost:27017'
        )

    def test_get_mongo_creds_for_profile(self):
        self.assertEqual(
            get_mongo_creds_for_profile('test', self.config_file),
            ('mongo.example.com:27017,mongo.example-primary.com:27017', 'testuser', 'testpassword')
        )
        self.assertEqual(
            get_mongo_creds_for_profile('local', self.config_file),
            ('localhost:27017', None, None)
        )
