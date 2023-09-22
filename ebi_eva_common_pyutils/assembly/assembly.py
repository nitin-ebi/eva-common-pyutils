# Copyright 2019 EMBL - European Bioinformatics Institute
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
from datetime import datetime
from functools import lru_cache

from ebi_eva_common_pyutils.logger import logging_config
from ebi_eva_common_pyutils.network_utils import json_request
from ebi_eva_common_pyutils.taxonomy.taxonomy import get_normalized_scientific_name_from_ensembl

logger = logging_config.get_logger(__name__)


def get_supported_asm_from_ensembl(tax_id: int) -> str:
    logger.info(f'Query Ensembl for species name using taxonomy {tax_id}')
    scientific_name_api_param = get_normalized_scientific_name_from_ensembl(tax_id)
    ENSEMBL_REST_API_URL = "http://rest.ensembl.org/info/assembly/{0}?content-type=application/json".format(
        scientific_name_api_param)
    response = json_request(ENSEMBL_REST_API_URL)
    assembly_accession_attribute = 'assembly_accession'
    if assembly_accession_attribute in response:
        return str(response.get(assembly_accession_attribute))
    return None


@lru_cache(maxsize=None)
def get_taxonomy_to_assembly_mapping_from_ensembl_rapid_release():
    """
    Returns a dict mapping taxonomy ID to assembly accession, choosing the most recently released,
    lexicographically last, non-alternate haplotype assembly when multiple are present.
    """
    list_data = json_request('https://ftp.ensembl.org/pub/rapid-release/species_metadata.json')
    results = {}
    for asm_data in list_data:
        tax_id = asm_data['taxonomy_id']
        asm_accession = asm_data['assembly_accession']
        strain = asm_data['strain']
        release_date = datetime.strptime(asm_data['release_date'], '%Y-%m-%d')

        # If we haven't seen this taxonomy before, just use this assembly
        if tax_id not in results:
            results[tax_id] = (asm_accession, release_date)
            continue

        # Skip alternate haplotype assemblies
        if strain and strain.lower() == 'alternate haplotype':
            continue
        current_assembly, current_date = results[tax_id]
        # Keep the more recent assembly, or the lexicographically last one if release dates are equal
        if current_date < release_date or (current_date == release_date and asm_accession > current_assembly):
            results[tax_id] = (asm_accession, release_date)

    return {key: val[0] for key, val in results.items()}


def get_supported_asm_from_ensembl_rapid_release(tax_id: int) -> str:
    # TODO: Replace with API call once supported
    rapid_release_data = get_taxonomy_to_assembly_mapping_from_ensembl_rapid_release()
    return rapid_release_data.get(tax_id, None)
