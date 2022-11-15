# Copyright 2022 EMBL - European Bioinformatics Institute
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

from ebi_eva_common_pyutils.logger import AppLogger
import requests
from retry import retry


class InternalServerError(Exception):
    pass


# TODO add the get methods
class ContigAliasClient(AppLogger):
    """
    Python client for interfacing with the contig alias service.
    Authentication is required if using admin endpoints.
    """

    def __init__(self, base_url, username=None, password=None):
        self.base_url = base_url
        # Only required for admin endpoints
        self.username = username
        self.password = password

    def check_auth(self):
        if self.username is None or self.password is None:
            raise ValueError('Need admin username and password for this method')

    @retry(InternalServerError, tries=3, delay=2, backoff=1.5, jitter=(1, 3))
    def insert_assembly(self, assembly):
        self.check_auth()
        full_url = os.path.join(self.base_url, f'v1/admin/assemblies/{assembly}')

        response = requests.put(full_url, auth=(self.username, self.password))
        if response.status_code == 200:
            self.info(f'Assembly accession {assembly} successfully added to Contig-Alias DB')
        elif response.status_code == 409:
            self.warning(f'Assembly accession {assembly} already exists in Contig-Alias DB. Response: {response.text}')
        elif response.status_code == 500:
            self.error(f'Could not save Assembly accession {assembly} to Contig-Alias DB. Error: {response.text}')
            raise InternalServerError
        else:
            self.error(f'Could not save Assembly accession {assembly} to Contig-Alias DB. Error: {response.text}')
            response.raise_for_status()

    @retry(InternalServerError, tries=3, delay=2, backoff=1.5, jitter=(1, 3))
    def delete_assembly(self, assembly):
        self.check_auth()
        full_url = os.path.join(self.base_url, f'v1/admin/assemblies/{assembly}')

        response = requests.delete(full_url, auth=(self.username, self.password))
        if response.status_code == 200:
            self.info(f'Assembly accession {assembly} successfully deleted from Contig-Alias DB')
        elif response.status_code == 500:
            self.error(f'Assembly accession {assembly} could not be deleted. Response: {response.text}')
            raise InternalServerError
        else:
            self.error(f'Assembly accession {assembly} could not be deleted. Response: {response.text}')
