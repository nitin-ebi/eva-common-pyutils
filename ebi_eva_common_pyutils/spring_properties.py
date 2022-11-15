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
from ebi_eva_common_pyutils.config_utils import get_primary_mongo_creds_for_profile, get_accession_pg_creds_for_profile,\
    get_count_service_creds_for_profile


class SpringPropertiesGenerator:
    """
    Class to generate Spring properties for various Spring Batch pipelines.
    These methods can be used to generate complete properties files entirely in Python; alternatively, certain
    properties can be left unfilled and supplied as command-line arguments (e.g. by a NextFlow process).
    """

    def __init__(self, maven_profile, private_settings_file):
        self.maven_profile = maven_profile
        self.private_settings_file = private_settings_file

    def _common_properties(self, assembly_accession):
        """Properties common to all Spring pipelines"""
        mongo_host, mongo_user, mongo_pass = get_primary_mongo_creds_for_profile(
            self.maven_profile, self.private_settings_file)
        pg_url, pg_user, pg_pass = get_accession_pg_creds_for_profile(self.maven_profile, self.private_settings_file)
        return f'''spring.datasource.driver-class-name=org.postgresql.Driver
spring.datasource.url={pg_url}
spring.datasource.username={pg_user}
spring.datasource.password={pg_pass}
spring.datasource.tomcat.max-active=3

spring.jpa.generate-ddl=true

spring.data.mongodb.host={mongo_host}
spring.data.mongodb.port=27017
spring.data.mongodb.database=eva_accession_sharded
spring.data.mongodb.username={mongo_user}
spring.data.mongodb.password={mongo_pass}

spring.data.mongodb.authentication-database=admin
mongodb.read-preference=secondaryPreferred
spring.main.web-application-type=none
spring.main.allow-bean-definition-overriding=true
spring.jpa.properties.hibernate.jdbc.lob.non_contextual_creation=true

parameters.chunkSize=1000
parameters.assemblyAccession={assembly_accession}
'''

    def _accessioning_properties(self, instance):
        """Properties common to accessioning and clustering pipelines."""
        counts_url, counts_username, counts_password = get_count_service_creds_for_profile(
            self.maven_profile, self.private_settings_file)
        return f'''
accessioning.instanceId=instance-{instance}
accessioning.submitted.categoryId=ss
accessioning.clustered.categoryId=rs

accessioning.monotonic.ss.blockSize=100000
accessioning.monotonic.ss.blockStartValue=5000000000
accessioning.monotonic.ss.nextBlockInterval=1000000000
accessioning.monotonic.rs.blockSize=100000
accessioning.monotonic.rs.blockStartValue=3000000000
accessioning.monotonic.rs.nextBlockInterval=1000000000

eva.count-stats.url={counts_url}
eva.count-stats.username={counts_username}
eva.count-stats.password={counts_password}
'''

    def get_remapping_extraction_properties(self, *, taxonomy='', source_assembly='', fasta='', assembly_report='',
                                            projects='', output_folder='.'):
        """Properties for remapping extraction pipeline."""
        return self._common_properties(source_assembly) + f'''
spring.batch.job.names=EXPORT_SUBMITTED_VARIANTS_JOB
parameters.taxonomy={taxonomy}
parameters.fasta={fasta}
parameters.assemblyReportUrl=file:{assembly_report}
parameters.projects={projects}
parameters.outputFolder={output_folder}
'''

    def get_remapping_ingestion_properties(self, *, source_assembly='', target_assembly='', vcf='', load_to='',
                                           remapping_version=1.0):
        """Properties for remapping ingestion pipeline."""
        return self._common_properties(target_assembly) + f'''
spring.batch.job.names=INGEST_REMAPPED_VARIANTS_FROM_VCF_JOB
parameters.vcf={vcf}
parameters.remappedFrom={source_assembly}
parameters.loadTo={load_to}
parameters.remappingVersion={remapping_version}
'''

    def get_clustering_properties(self, *, instance,
                                  job_name='', target_assembly='', rs_report_path='', projects='',
                                  project_accession='', vcf='', source_assembly=''):
        """Properties common to all clustering pipelines, though not all are always used."""
        return self._common_properties(target_assembly) + self._accessioning_properties(instance) + f'''
spring.batch.job.names={job_name}
parameters.projects={projects}
parameters.projectAccession={project_accession}
parameters.vcf={vcf}
parameters.remappedFrom={source_assembly}
parameters.rsReportPath={rs_report_path}
'''
