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

from urllib.parse import quote_plus
from lxml import etree as et


class EVAPrivateSettingsXMLConfig:
    config_data = None

    def __init__(self, settings_xml_file: str):
        with open(settings_xml_file) as xml_file_handle:
            self.config_data = et.parse(xml_file_handle)

    def get_value_with_xpath(self, location: str):
        etree = self.config_data.getroot()
        result = etree.xpath(location)
        if not result:
            raise ValueError("Invalid XPath location: " + location)
        return result


def get_pg_metadata_uri_for_eva_profile(eva_profile_name: str, settings_xml_file: str):
    config = EVAPrivateSettingsXMLConfig(settings_xml_file)
    xpath_location_template = '//settings/profiles/profile/id[text()="{0}"]/../properties/{1}/text()'
    # Format is jdbc:postgresql://host:port/db
    metadata_db_jdbc_url = config.get_value_with_xpath(
        xpath_location_template.format(eva_profile_name, "eva.evapro.jdbc.url"))[0]
    return metadata_db_jdbc_url.split("jdbc:")[-1]


def get_mongo_uri_for_eva_profile(eva_profile_name: str, settings_xml_file: str):
    config = EVAPrivateSettingsXMLConfig(settings_xml_file)
    xpath_location_template = '//settings/profiles/profile/id[text()="{0}"]/../properties/{1}/text()'
    # Format is host1:port1,host2:port2
    mongo_hosts_and_ports = config.get_value_with_xpath(
        xpath_location_template.format(eva_profile_name, "eva.mongo.host"))[0]
    username = config.get_value_with_xpath(
        xpath_location_template.format(eva_profile_name, "eva.mongo.user"))[0]
    password = config.get_value_with_xpath(
        xpath_location_template.format(eva_profile_name, "eva.mongo.passwd"))[0]
    authentication_db = config.get_value_with_xpath(
        xpath_location_template.format(eva_profile_name, "eva.mongo.auth.db"))[0]
    return "mongodb://{0}:{1}@{2}/{3}".format(username, quote_plus(password), mongo_hosts_and_ports, authentication_db)
