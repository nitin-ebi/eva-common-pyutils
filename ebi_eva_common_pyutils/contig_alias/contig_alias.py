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


CONTING_ALIAS_URL = 'https://www.ebi.ac.uk/eva/webservices/contig-alias'


# TODO add the get methods
class ContigAliasClient(AppLogger):
    """
    Python client for interfacing with the contig alias service.
    Authentication is required if using admin endpoints.
    """

    def __init__(self, base_url=None, username=None, password=None, default_page_size=1000):
        if base_url:
            self.base_url = base_url
        else:
            self.base_url = os.environ.get('CONTING_ALIAS_URL') or CONTING_ALIAS_URL
        # Used for get method
        self.default_page_size=default_page_size
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

    @retry(tries=3, delay=2, backoff=1.2, jitter=(1, 3))
    def _get_page_for_contig_alias_url(self, sub_url, page=0):
        """queries the contig alias to retrieve the page of the provided url"""
        url = f'{self.base_url}/{sub_url}?page={page}&size={self.default_page_size}'
        response = requests.get(url, headers={'accept': 'application/json'})
        response.raise_for_status()
        response_json = response.json()
        return response_json

    def _depaginate_iter(self, sub_url, entity_to_retrieve):
        """Generator that provides the contigs in the assembly requested."""
        page = 0
        response_json = self._get_page_for_contig_alias_url(sub_url, page=page)
        for entity in response_json.get('_embedded', {}).get(entity_to_retrieve, []):
            yield entity
        while 'next' in response_json['_links']:
            page += 1
            response_json = self._get_page_for_contig_alias_url(sub_url, page=page)
            for entity in response_json.get('_embedded', {}).get(entity_to_retrieve, []):
                yield entity

    def assembly_contig_iter(self, assembly_accession):
        """Generator that provides the contigs in the assembly requested."""
        sub_url = f'v1/assemblies/{assembly_accession}/chromosomes'
        return self._depaginate_iter(sub_url, 'chromosomeEntities')

    def assembly(self, assembly_accession):
        """provides the description of the requested assembly."""
        sub_url = f'v1/assemblies/{assembly_accession}'
        response_json = self._get_page_for_contig_alias_url(sub_url)
        return response_json.get('_embedded', {}).get('assemblyEntities', [])[0]

    def contig_iter(self, insdc_accession):
        sub_url = f'v1/chromosomes/genbank/{insdc_accession}'
        return self._depaginate_iter(sub_url, 'chromosomeEntities')
