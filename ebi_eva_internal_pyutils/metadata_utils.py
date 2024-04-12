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
import datetime
import re
from urllib.parse import urlsplit

import psycopg2

from ebi_eva_common_pyutils.assembly_utils import is_patch_assembly
from ebi_eva_internal_pyutils.config_utils import get_metadata_creds_for_profile
from ebi_eva_common_pyutils.ena_utils import get_scientific_name_and_common_name
from ebi_eva_common_pyutils.logger import logging_config
from ebi_eva_common_pyutils.ncbi_utils import get_ncbi_assembly_name_from_term
from ebi_eva_internal_pyutils.pg_utils import get_result_cursor, get_all_results_for_query, execute_query
from ebi_eva_common_pyutils.taxonomy.taxonomy import get_scientific_name_from_ensembl

logger = logging_config.get_logger(__name__)
SUPPORTED_ASSEMBLY_TRACKER_TABLE = "evapro.supported_assembly_tracker"


def get_metadata_connection_handle(profile, settings_xml_file):
    pg_url, pg_user, pg_pass = get_metadata_creds_for_profile(profile, settings_xml_file)
    return psycopg2.connect(urlsplit(pg_url).path, user=pg_user, password=pg_pass)


def get_db_conn_for_species(species_db_info):
    db_name = "dbsnp_{0}".format(species_db_info["dbsnp_build"])
    pg_conn = psycopg2.connect("dbname='{0}' user='{1}' host='{2}'  port={3}".
                               format(db_name, "dbsnp", species_db_info["pg_host"], species_db_info["pg_port"]))
    return pg_conn


def get_species_info(metadata_connection_handle, dbsnp_species_name="all"):
    get_species_info_query = "SELECT DISTINCT database_name, scientific_name, dbsnp_build, pg_host, pg_port " \
                             "FROM dbsnp_ensembl_species.import_progress a " \
                             "JOIN dbsnp_ensembl_species.dbsnp_build_instance b " \
                             "ON b.dbsnp_build = a.ebi_pg_dbsnp_build "
    if dbsnp_species_name != "all":
        get_species_info_query += "where database_name = '{0}' ".format(dbsnp_species_name)
    get_species_info_query += "order by database_name"

    pg_cursor = get_result_cursor(metadata_connection_handle, get_species_info_query)
    species_set = [{"database_name": result[0], "scientific_name": result[1], "dbsnp_build":result[2],
                    "pg_host":result[3], "pg_port":result[4]}
                   for result in pg_cursor.fetchall()]
    pg_cursor.close()
    return species_set


# Get connection information for each Postgres instance of the dbSNP mirror
def get_dbsnp_mirror_db_info(pg_metadata_dbname, pg_metadata_user, pg_metadata_host):
    with psycopg2.connect("dbname='{0}' user='{1}' host='{2}'".format(pg_metadata_dbname, pg_metadata_user,
                                                                      pg_metadata_host)) as pg_conn:
        dbsnp_mirror_db_info_query = "SELECT * FROM dbsnp_ensembl_species.dbsnp_build_instance"
        dbsnp_mirror_db_info = [{"dbsnp_build": result[0], "pg_host": result[1], "pg_port": result[2]}
                                for result in get_all_results_for_query(pg_conn, dbsnp_mirror_db_info_query)]
    return dbsnp_mirror_db_info


def get_taxonomy_code_from_metadata(metadata_connection_handle, taxonomy):
    """
    Retrieve an existing taxonomy code registered in the metadata database.
    """
    query = f"SELECT DISTINCT t.taxonomy_code FROM taxonomy t WHERE t.taxonomy_id = {taxonomy}"
    rows = get_all_results_for_query(metadata_connection_handle, query)
    if len(rows) == 0:
        return None
    elif len(rows) > 1:
        options = ', '.join(rows)
        raise ValueError(f'More than one possible code for taxonomy {taxonomy} found: {options}')
    return rows[0][0]


def get_assembly_code_from_metadata(metadata_connection_handle, assembly):
    """
    Retrieve an existing assembly code registered in the metadata database.
    """
    query = f"SELECT DISTINCT assembly_code FROM assembly WHERE assembly_accession='{assembly}'"
    rows = get_all_results_for_query(metadata_connection_handle, query)
    if len(rows) == 0:
        return None
    elif len(rows) > 1:
        options = ', '.join([row for row, in rows])
        raise ValueError(f'More than one possible code for assembly {assembly} found: {options}')
    return rows[0][0]


def build_variant_warehouse_database_name(taxonomy_code, assembly_code):
    if taxonomy_code and assembly_code:
        return f'eva_{taxonomy_code}_{assembly_code}'
    return None


def resolve_existing_variant_warehouse_db_name(metadata_connection_handle, assembly, taxonomy):
    """
    Retrieve an existing database name by combining the taxonomy_code and assembly code registered in the metadata
    database.
    """
    return build_variant_warehouse_database_name(
        get_taxonomy_code_from_metadata(metadata_connection_handle, taxonomy),
        get_assembly_code_from_metadata(metadata_connection_handle, assembly)
    )


# For backward compatibility
get_variant_warehouse_db_name_from_assembly_and_taxonomy = resolve_existing_variant_warehouse_db_name


def get_assembly_code(metadata_connection_handle, assembly, ncbi_api_key=None):
    assembly_code = get_assembly_code_from_metadata(metadata_connection_handle, assembly)
    if not assembly_code:
        assembly_name = get_ncbi_assembly_name_from_term(assembly, api_key=ncbi_api_key)
        # If the assembly is a patch assembly ex: GRCh37.p8, drop the trailing patch i.e., just return grch37
        if is_patch_assembly(assembly):
            assembly_name = re.sub('\\.p[0-9]+$', '', assembly_name.lower())
        assembly_code = re.sub('[^0-9a-zA-Z]+', '', assembly_name.lower())
    return assembly_code


def build_taxonomy_code(scientific_name):
    """Given a scientific name like "Zea mays", the corresponding taxonomy code should be zmays"""
    return scientific_name[0].lower() + re.sub('[^0-9a-zA-Z]+', '', ''.join(scientific_name.split()[1:])).lower()


def get_taxonomy_code(metadata_connection_handle, taxonomy):
    taxonomy_code = get_taxonomy_code_from_metadata(metadata_connection_handle, taxonomy)
    if not taxonomy_code:
        scientific_name = get_scientific_name_from_ensembl(taxonomy)
        taxonomy_code = build_taxonomy_code(scientific_name)
    return taxonomy_code


def resolve_variant_warehouse_db_name(metadata_connection_handle, assembly, taxonomy, ncbi_api_key=None):
    """
    Retrieve the database name for this taxonomy/assembly pair whether it exists or not.
    It will use existing taxonomy code or assembly code if available in the metadata database.
    """
    taxonomy_code = get_taxonomy_code(metadata_connection_handle, taxonomy)
    assembly_code = get_assembly_code(metadata_connection_handle, assembly, ncbi_api_key=ncbi_api_key)
    return build_variant_warehouse_database_name(taxonomy_code, assembly_code)


def insert_new_assembly_and_taxonomy(metadata_connection_handle, assembly_accession, taxonomy_id, eva_species_name=None,
                                     in_accessioning=True, ncbi_api_key=None):
    """
    This script adds new assemblies and taxonomies to EVAPRO.
    You can also add the assembly with a different taxonomy if you provide the
    taxonomy parameters. Example taxonomy page:
    https://www.ebi.ac.uk/ena/data/view/Taxon:9031

    :param assembly_accession: Assembly accession (Example: GCA_000002315.3)
    :param metadata_connection_handle: Metadata DB connection
    :param taxonomy_id: Taxonomy id (Example: 9031)
    :param eva_species_name: EVA species name (Example: chicken).
        Not required if the taxonomy exists or ENA has a common name available.
    :param in_accessioning: Flag that this assembly is in the accessioning data store.
    """
    # check if assembly is already in EVAPRO, adding it if not
    assembly_set_id = get_assembly_set_from_metadata(metadata_connection_handle, taxonomy_id, assembly_accession)
    if assembly_set_id is None:
        assembly_name = get_ncbi_assembly_name_from_term(assembly_accession, api_key=ncbi_api_key)
        ensure_taxonomy_is_in_evapro(metadata_connection_handle, taxonomy_id, eva_species_name)
        assembly_code = get_assembly_code(metadata_connection_handle, assembly_accession)
        insert_assembly_in_evapro(metadata_connection_handle, taxonomy_id, assembly_accession, assembly_name, assembly_code)

    update_accessioning_status(metadata_connection_handle, assembly_accession, in_accessioning)
    metadata_connection_handle.commit()


def ensure_taxonomy_is_in_evapro(metadata_connection_handle, taxonomy, eva_species_name=None):
    if is_taxonomy_in_evapro(metadata_connection_handle, taxonomy):
        logger.debug('Taxonomy {} is already in the database'.format(taxonomy))
    else:
        logger.info("Taxonomy {} not present in EVAPRO. Adding taxonomy ...".format(taxonomy))
        scientific_name, common_name = get_scientific_name_and_common_name(taxonomy)
        taxonomy_code = build_taxonomy_code(scientific_name)
        # If a common name cannot be found then we should  use the scientific name
        eva_species_name = eva_species_name or common_name or scientific_name
        insert_taxonomy(metadata_connection_handle, taxonomy, scientific_name, common_name, taxonomy_code, eva_species_name)


def insert_assembly_in_evapro(metadata_connection_handle, taxonomy_id, assembly_accession, assembly_name, assembly_code):
    cur = metadata_connection_handle.cursor()
    cur.execute('INSERT INTO evapro.assembly_set(taxonomy_id, assembly_name, assembly_code) VALUES (%s, %s, %s)',
                (taxonomy_id, assembly_name, assembly_code))

    # get the assembly_set_id that was autogenerated in the row that we just inserted in assembly_set
    assembly_set_id = get_all_results_for_query(metadata_connection_handle,
                                    'SELECT assembly_set_id FROM evapro.assembly_set '
                                    'WHERE taxonomy_id={} and assembly_name=\'{}\' and assembly_code=\'{}\''
                                                .format(taxonomy_id, assembly_name, assembly_code))[0][0]

    assembly_chain = assembly_accession.split('.')[0]
    assembly_version = assembly_accession.split('.')[1]
    cur.execute('INSERT INTO evapro.accessioned_assembly('
                'assembly_set_id, assembly_accession, assembly_chain, assembly_version) VALUES (%s,%s,%s,%s)',
                (assembly_set_id, assembly_accession, assembly_chain, assembly_version))

    logger.info('New assembly added with assembly_set_id: {0}'.format(assembly_set_id))
    return assembly_set_id


def update_accessioning_status(metadata_connection_handle, assembly_accession, in_accessioning_flag):
    cur = metadata_connection_handle.cursor()
    # Only insert assembly accessions which are NOT already in the assembly_accessioning_store_status table
    assembly_accessioning_store_insert_query = "INSERT INTO evapro.assembly_accessioning_store_status " \
                                               "SELECT * FROM (SELECT " \
                                               "cast('{0}' as text) as assembly_accession" \
                                               ", cast('{1}' as boolean) as loaded) temp " \
                                               "WHERE assembly_accession NOT IN " \
                                               "(SELECT assembly_accession FROM " \
                                               "evapro.assembly_accessioning_store_status)" \
                                               .format(assembly_accession, in_accessioning_flag)
    cur.execute(assembly_accessioning_store_insert_query)


def get_assembly_set_from_metadata(metadata_connection_handle, taxonomy, assembly_accession):
    query = (f"SELECT acc.assembly_set_id "
             f"FROM evapro.accessioned_assembly acc "
             f"JOIN assembly_set asm on acc.assembly_set_id = asm.assembly_set_id "
             f"WHERE assembly_accession='{assembly_accession}' AND taxonomy_id={taxonomy}")
    rows = get_all_results_for_query(metadata_connection_handle, query)

    if len(rows) == 1:
        return rows[0][0]
    elif len(rows) == 0:
        return None
    else:
        raise ValueError('Inconsistent database state: several assembly_set_ids for the same taxonomy ({}) and '
                         'assembly accession ({}): {}'.format(taxonomy, assembly_accession, rows))


def is_taxonomy_in_evapro(metadata_connection_handle, taxonomy_id):
    taxonomy_query = 'SELECT taxonomy_id FROM evapro.taxonomy WHERE taxonomy_id={}'.format(taxonomy_id)
    taxonomy_ids_in_evapro = get_all_results_for_query(metadata_connection_handle, taxonomy_query)
    return len(taxonomy_ids_in_evapro) > 0


def insert_taxonomy(metadata_connection_handle, taxonomy_id, scientific_name, common_name, taxonomy_code, eva_species_name):
    if taxonomy_code is None or eva_species_name is None:
        raise ValueError('Error: taxonomy code ({}) and EVA taxonomy name ({}) are required '
                         'for inserting a taxonomy'.format(taxonomy_code, eva_species_name))
    cur = metadata_connection_handle.cursor()
    cur.execute('INSERT INTO evapro.taxonomy(taxonomy_id, common_name, scientific_name, taxonomy_code, eva_name) '
                'VALUES (%s, %s, %s, %s, %s)',
                (taxonomy_id, common_name, scientific_name, taxonomy_code, eva_species_name))
    logger.info('New taxonomy {} added'.format(taxonomy_id))


def add_to_supported_assemblies(metadata_connection_handle, source_of_assembly: str, target_assembly: str,
                                taxonomy_id: int):
    today = datetime.date.today().strftime('%Y-%m-%d')
    # First check if the current assembly is already target - if so don't do anything
    current_query = (
        f"SELECT assembly_id FROM {SUPPORTED_ASSEMBLY_TRACKER_TABLE} "
        f"WHERE taxonomy_id={taxonomy_id} AND current=true;"
    )
    results = get_all_results_for_query(metadata_connection_handle, current_query)
    if len(results) > 0 and results[0][0] == target_assembly:
        logger.warning(f'Current assembly for taxonomy {taxonomy_id} is already {target_assembly}!')
        return

    # Deprecate the last current assembly
    update_query = (
        f"UPDATE {SUPPORTED_ASSEMBLY_TRACKER_TABLE} "
        f"SET current=false, end_date='{today}' "
        f"WHERE taxonomy_id={taxonomy_id} AND current=true;"
    )
    execute_query(metadata_connection_handle, update_query)

    # Then insert the new assembly
    insert_query = (
        f"INSERT INTO {SUPPORTED_ASSEMBLY_TRACKER_TABLE} "
        f"(taxonomy_id, source, assembly_id, current, start_date) "
        f"VALUES({taxonomy_id}, '{source_of_assembly}', '{target_assembly}', true, '{today}');"
    )
    execute_query(metadata_connection_handle, insert_query)