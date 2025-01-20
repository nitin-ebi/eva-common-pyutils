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
from collections import defaultdict

from ebi_eva_internal_pyutils.config_utils import get_mongo_creds_for_profile, get_accession_pg_creds_for_profile, \
    get_count_service_creds_for_profile, get_properties_from_xml_file, get_variant_load_job_tracker_creds_for_profile


class SpringPropertiesGenerator:
    """
    Class to generate Spring properties for various Spring Batch pipelines.
    These methods can be used to generate complete properties files entirely in Python; alternatively, certain
    properties can be left unfilled and supplied as command-line arguments (e.g. by a NextFlow process).
    """

    def __init__(self, maven_profile, private_settings_file):
        self.maven_profile = maven_profile
        self.private_settings_file = private_settings_file

    @staticmethod
    def _format(*key_value_maps):
        all_params = defaultdict(list)
        for key_value_map in key_value_maps:
            for key in key_value_map:
                if key_value_map[key] is not None:
                    all_params[key.split('.')[0]].append(f'{key}={key_value_map[key]}')
        lines = []
        for key_type in all_params:
            for line in all_params[key_type]:
                lines.append(line)
            lines.append('')

        return '\n'.join(lines)

    @staticmethod
    def _format_str(string, param):
        if param is None:
            return None
        elif not param:
            return ''
        else:
            return string.format(param)

    def _mongo_properties(self):
        mongo_host, mongo_user, mongo_pass = get_mongo_creds_for_profile(
            self.maven_profile, self.private_settings_file)
        return {
            'spring.data.mongodb.host': mongo_host,
            # Don't specify port if hosts already have the port encoded in them
            # Also see https://github.com/EBIvariation/variation-commons/blob/329d4fa18da73bdad419ca5456b9897c059e33f0/variation-commons-mongodb/src/main/java/uk/ac/ebi/eva/commons/mongodb/utils/MongoClientURIBuilder.java#L44
            'spring.data.mongodb.port': '' if ':' in mongo_host else 27017,
            'spring.data.mongodb.username': '' if mongo_user is None else mongo_user,
            'spring.data.mongodb.password': '' if mongo_pass is None else mongo_pass,
            'spring.data.mongodb.authentication-database': 'admin',
        }

    def _variant_load_job_tracker_properties(self):
        variant_url, variant_user, variant_pass = get_variant_load_job_tracker_creds_for_profile(self.maven_profile,
                                                                                                 self.private_settings_file)
        return {
            'job.repository.url': variant_url,
            'job.repository.username': variant_user,
            'job.repository.password': variant_pass,
        }

    def _count_stats_properties(self):
        counts_url, counts_username, counts_password = get_count_service_creds_for_profile(
            self.maven_profile, self.private_settings_file)
        return {
            'eva.count-stats.url': counts_url,
            'eva.count-stats.username': counts_username,
            'eva.count-stats.password': counts_password
        }

    def _common_properties(self, *, read_preference='primary', chunk_size=100, max_pool_size=2):
        """Properties common to all Spring pipelines"""
        props = {
            'spring.datasource.driver-class-name': 'org.postgresql.Driver',
            'spring.datasource.tomcat.max-active': 3,
            'spring.jpa.generate-ddl': 'true',

            'mongodb.read-preference': read_preference,

            'spring.main.web-application-type': 'none',
            'spring.main.allow-bean-definition-overriding': 'true',
            'spring.jpa.properties.hibernate.jdbc.lob.non_contextual_creation': 'true',
            'spring.jpa.properties.hibernate.temp.use_jdbc_metadata_defaults': 'false',
            'spring.jpa.database-platform': 'org.hibernate.dialect.PostgreSQL9Dialect',
            'parameters.chunkSize': chunk_size,
            'spring.datasource.hikari.maximum-pool-size': max_pool_size
        }
        merge = {**self._mongo_properties(), **self._count_stats_properties(), **props}
        return merge

    def _common_accessioning_properties(self, assembly_accession, read_preference, chunk_size):
        pg_url, pg_user, pg_pass = get_accession_pg_creds_for_profile(self.maven_profile, self.private_settings_file)
        accession_db = get_properties_from_xml_file(
            self.maven_profile, self.private_settings_file)['eva.accession.mongo.database']
        props = {
            'spring.datasource.url': pg_url,
            'spring.datasource.username': pg_user,
            'spring.datasource.password': pg_pass,
            'spring.data.mongodb.database': accession_db,
            'parameters.assemblyAccession': assembly_accession,
        }

        merge = {**self._common_properties(read_preference=read_preference, chunk_size=chunk_size), **props}
        return merge

    def _common_accessioning_clustering_properties(self, *, assembly_accession, read_preference, chunk_size):
        """Properties common to accessioning and clustering pipelines."""
        props = {
            'accessioning.submitted.categoryId': 'ss',
            'accessioning.clustered.categoryId': 'rs',
            'accessioning.monotonic.ss.blockSize': 100000,
            'accessioning.monotonic.ss.blockStartValue': 5000000000,
            'accessioning.monotonic.ss.nextBlockInterval': 1000000000,
            'accessioning.monotonic.rs.blockSize': 100000,
            'accessioning.monotonic.rs.blockStartValue': 3000000000,
            'accessioning.monotonic.rs.nextBlockInterval': 1000000000,
            # This value is not used but is required to create beans in Java
            'recovery.cutoff.days': 9999999
        }
        merge = {**self._common_accessioning_properties(assembly_accession, read_preference, chunk_size), **props}
        return merge

    def get_accessioning_properties(self, *, target_assembly=None, fasta=None, assembly_report=None,
                                    project_accession=None, aggregation='BASIC', taxonomy_accession=None,
                                    vcf_file='', output_vcf='', chunk_size=100):
        """Properties for accessioning pipeline."""
        return self._format(
            self._common_accessioning_clustering_properties(assembly_accession=target_assembly,
                                                            read_preference='secondaryPreferred',
                                                            chunk_size=chunk_size),
            {
                'spring.batch.job.names': 'CREATE_SUBSNP_ACCESSION_JOB',
                'parameters.assemblyReportUrl': self._format_str('file:{0}', assembly_report),
                'parameters.contigNaming': 'NO_REPLACEMENT',
                'parameters.fasta': fasta,
                'parameters.forceRestart': 'false',
                'parameters.projectAccession': project_accession,
                'parameters.taxonomyAccession': taxonomy_accession,
                'parameters.vcfAggregation': aggregation,
                'parameters.vcf': vcf_file,
                'parameters.outputVcf': output_vcf
            },
        )

    def get_clustering_properties(self, *, read_preference='primary', job_name=None, source_assembly='',
                                  target_assembly='', rs_report_path='', rs_acc_file='', duplicate_rs_acc_file='',
                                  projects='', project_accession='', vcf=''):
        """Properties common to all clustering pipelines, though not all are always used."""
        return self._format(
            self._common_accessioning_clustering_properties(assembly_accession=target_assembly,
                                                            read_preference=read_preference, chunk_size=100),
            {
                'spring.batch.job.names': job_name,
                'parameters.remappedFrom': source_assembly,
                'parameters.projects': projects,
                'parameters.projectAccession': project_accession,
                'parameters.vcf': vcf,
                'parameters.rsReportPath': rs_report_path,
                'parameters.rsAccFile': rs_acc_file,
                'parameters.duplicateRSAccFile': duplicate_rs_acc_file,
            }
        )

    def get_remapping_extraction_properties(self, *, taxonomy=None, source_assembly=None, fasta=None,
                                            assembly_report=None,
                                            projects='', output_folder=None):
        """Properties for remapping extraction pipeline."""
        return self._format(
            self._common_accessioning_properties(assembly_accession=source_assembly,
                                                 read_preference='secondaryPreferred',
                                                 chunk_size=1000),
            {
                'spring.batch.job.names': 'EXPORT_SUBMITTED_VARIANTS_JOB',
                'parameters.taxonomy': taxonomy,
                'parameters.fasta': fasta,
                'parameters.assemblyReportUrl': self._format_str('file:{0}', assembly_report),
                'parameters.projects': projects,
                'parameters.outputFolder': output_folder
            })

    def get_remapping_ingestion_properties(self, *, source_assembly=None, target_assembly=None, vcf=None, load_to=None,
                                           remapping_version=1.0):
        """Properties for remapping ingestion pipeline."""
        return self._format(
            self._common_accessioning_properties(assembly_accession=target_assembly,
                                                 read_preference='secondaryPreferred',
                                                 chunk_size=1000),
            {
                'spring.batch.job.names': 'INGEST_REMAPPED_VARIANTS_FROM_VCF_JOB',
                'parameters.vcf': vcf,
                'parameters.remappedFrom': source_assembly,
                'parameters.loadTo': load_to,
                'parameters.remappingVersion': remapping_version,
            }
        )

    def get_release_properties(self, *, job_name=None, assembly_accession=None, taxonomy_accession=None, fasta=None,
                               assembly_report=None, contig_naming=None, output_folder=None, accessioned_vcf=None,
                               temp_mongo_db=None):
        common_props = self._common_accessioning_properties(assembly_accession=assembly_accession,
                                                            read_preference='secondaryPreferred', chunk_size=1000)
        # For release in Embassy only
        if temp_mongo_db:
            common_props['spring.data.mongodb.database'] = temp_mongo_db
            common_props['mongodb.read-preference'] = 'primaryPreferred'
            common_props.pop('spring.data.mongodb.host')
            common_props.pop('spring.data.mongodb.port')
            common_props.pop('spring.data.mongodb.username')
            common_props.pop('spring.data.mongodb.password')
        return self._format(
            common_props,
            {
                'spring.batch.job.names': job_name,
                'parameters.taxonomyAccession': taxonomy_accession,
                'parameters.contigNaming': contig_naming,
                'parameters.fasta': fasta,
                'parameters.assemblyReportUrl': self._format_str('file:{0}', assembly_report),
                'parameters.outputFolder': output_folder,
                'parameters.accessionedVcf': '' if accessioned_vcf is None else accessioned_vcf,
                'logging.level.uk.ac.ebi.eva.accession.release': 'INFO'
            })

    def _common_eva_pipeline_properties(self, opencga_path, read_preference='secondaryPreferred'):
        files_collection = get_properties_from_xml_file(
            self.maven_profile, self.private_settings_file)['eva.mongo.collections.files']
        annotation_metadata_collection = get_properties_from_xml_file(
            self.maven_profile, self.private_settings_file)['eva.mongo.collections.annotation-metadata']
        annotation_collection = get_properties_from_xml_file(
            self.maven_profile, self.private_settings_file)['eva.mongo.collections.annotations']
        variants_collection = get_properties_from_xml_file(
            self.maven_profile, self.private_settings_file)['eva.mongo.collections.variants']
        job_tracker_properties = self._variant_load_job_tracker_properties()
        props = {
            'spring.profiles.active': 'production,mongo',
            'spring.profiles.include': 'variant-writer-mongo,variant-annotation-mongo',

            'spring.data.mongodb.authentication-mechanism': 'SCRAM-SHA-1',
            'job.repository.driverClassName': 'org.postgresql.Driver',

            'db.collections.variants.name': variants_collection,
            'db.collections.files.name': files_collection,
            'db.collections.annotation-metadata.name': annotation_metadata_collection,
            'db.collections.annotations.name': annotation_collection,

            'app.opencga.path': opencga_path,
            'config.restartability.allow': 'false',
            'config.db.read-preference': read_preference,

            'logging.level.embl.ebi.variation.eva': 'DEBUG',
            'logging.level.org.opencb.opencga': 'DEBUG',
            'logging.level.org.springframework': 'INFO',
        }

        merge = {**self._common_properties(read_preference=read_preference, chunk_size=100), **props,
                 **job_tracker_properties}
        return merge

    def get_accession_import_properties(self, opencga_path, read_preference='secondaryPreferred'):
        return self._format(self._common_eva_pipeline_properties(opencga_path, read_preference))

    def get_variant_load_properties(self, project_accession, study_name, output_dir, annotation_dir, stats_dir,
                                    vep_cache_path, opencga_path, read_preference='secondaryPreferred'):
        return self._format(
            self._common_eva_pipeline_properties(opencga_path, read_preference),
            {
                'annotation.overwrite': False,
                'app.vep.cache.path': vep_cache_path,
                'app.vep.num-forks': 4,
                'app.vep.timeout': 500,
                'config.chunk.size': 200,

                'input.study.id': project_accession,
                'input.study.name': study_name,
                'input.study.type': 'COLLECTION',

                'output.dir': str(output_dir),
                'output.dir.annotation': str(annotation_dir),
                'output.dir.statistics': str(stats_dir),

                'statistics.skip': False
            },
        )
