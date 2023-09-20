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

import re

from ebi_eva_common_pyutils.logger import logging_config as log_cfg
from ebi_eva_common_pyutils.ncbi_utils import retrieve_species_scientific_name_from_tax_id_ncbi
from ebi_eva_common_pyutils.network_utils import json_request
from ebi_eva_common_pyutils.pg_utils import get_all_results_for_query

logger = log_cfg.get_logger(__name__)
logger.setLevel('INFO')

def get_scientific_name_from_ensembl(taxonomy_id: int) -> str:
    ENSEMBL_REST_API_URL = "https://rest.ensembl.org/taxonomy/id/{0}?content-type=application/json".format(taxonomy_id)
    response = json_request(ENSEMBL_REST_API_URL)
    if "scientific_name" not in response:
        logger.warning("Scientific name could not be found for taxonomy {0} using the Ensembl API URL: {1}"
                       .format(taxonomy_id, ENSEMBL_REST_API_URL))
        return None
    else:
        logger.info(f'Retrieved scientific name {response["scientific_name"]} from Ensembl for taxonomy {taxonomy_id}')
        return response["scientific_name"]


def normalise_taxon_scientific_name(taxon_name):
    """
    Match Ensembl representation
    See Clostridium sp. SS2/1 represented as clostridium_sp_ss2_1 in
    ftp://ftp.ensemblgenomes.org/pub/bacteria/release-48/fasta/bacteria_25_collection/clostridium_sp_ss2_1/
    """
    return re.sub('[^0-9a-zA-Z]+', '_', taxon_name.lower())


def get_normalized_scientific_name(taxonomy_id, private_config_xml_file, profile):
    """Get the scientific name for that taxon"""
    return normalise_taxon_scientific_name(get_scientific_name_from_taxonomy(taxonomy_id,
                                                                        private_config_xml_file=private_config_xml_file,
                                                                        profile=profile))

def get_scientific_name_from_eva(taxonomy_id, private_config_xml_file, profile):
    from ebi_eva_common_pyutils.metadata_utils import get_metadata_connection_handle
    with get_metadata_connection_handle(profile, private_config_xml_file) as pg_conn:
        return get_scientific_name_from_eva_using_metadata_connection(taxonomy_id, pg_conn)

def get_scientific_name_from_eva_using_metadata_connection(taxonomy_id, metadata_connection_handle):
    query = f"""select scientific_name from evapro.taxonomy where taxonomy_id={taxonomy_id}"""
    result = get_all_results_for_query(metadata_connection_handle, query)
    if not result:
        return None
    else:
        return result[0][0]

def get_scientific_name_from_taxonomy(taxonomy_id, private_config_xml_file=None, profile='production_processing', metadata_connection_handle=None):
    """
    Search for a species scientific name based on the taxonomy id.
    Will first attempt to retrieve from EVA Database, then Ensembl and then NCBI, if not found.
    """
    if not private_config_xml_file and not metadata_connection_handle:
        raise Exception('To get scientific name from EVA DB, either provide settings_file or connection to metadata db')

    if metadata_connection_handle:
        scientific_name = get_scientific_name_from_eva_using_metadata_connection(taxonomy_id, metadata_connection_handle)
    else:
        scientific_name = get_scientific_name_from_eva(taxonomy_id, private_config_xml_file, profile)

    if not scientific_name:
        scientific_name = get_scientific_name_from_ensembl(taxonomy_id)
        if not scientific_name:
            scientific_name = retrieve_species_scientific_name_from_tax_id_ncbi(taxonomy_id)

    return scientific_name