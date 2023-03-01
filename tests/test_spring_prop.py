import os
from unittest import TestCase

from ebi_eva_common_pyutils.spring_properties import SpringPropertiesGenerator
from tests.test_common import TestCommon


class TestSpringPropertiesGenerator(TestCommon):

    def setUp(self) -> None:
        private_file = os.path.join(self.resources_folder, 'private_settings.xml')
        self.prop = SpringPropertiesGenerator(maven_profile='dummy', private_settings_file=private_file)

    def test_get_remapping_extraction_properties(self):

        expected = '''spring.datasource.driver-class-name=org.postgresql.Driver
spring.datasource.url=jdbc:postgresql://host1.example.com:5432/accjtdb
spring.datasource.password=accpassword
spring.datasource.tomcat.max-active=3
spring.jpa.generate-ddl=true
spring.data.mongodb.host=mongos-host2.example.com
spring.data.mongodb.port=27017
spring.data.mongodb.database=eva_accession_sharded
spring.data.mongodb.username=mongouser
spring.data.mongodb.password=mongopassword
spring.data.mongodb.authentication-database=admin
spring.main.web-application-type=none
spring.main.allow-bean-definition-overriding=true
spring.jpa.properties.hibernate.jdbc.lob.non_contextual_creation=true
spring.batch.job.names=EXPORT_SUBMITTED_VARIANTS_JOB

mongodb.read-preference=secondaryPreferred

parameters.chunkSize=1000
parameters.assemblyAccession=GCA_00000001.1
parameters.taxonomy=9906
parameters.fasta=/path/to/fasta.fa
parameters.assemblyReportUrl=file:/path/to/assembly_report.txt
parameters.projects=PRJEB0001
parameters.outputFolder=/path/to/output_folder
'''
        assert self.prop.get_remapping_extraction_properties(
            taxonomy=9906, source_assembly='GCA_00000001.1', fasta='/path/to/fasta.fa',
            assembly_report='/path/to/assembly_report.txt', projects='PRJEB0001', output_folder='/path/to/output_folder'
        ) == expected

    def test_get_remapping_ingestion_properties(self):
        expected = '''spring.datasource.driver-class-name=org.postgresql.Driver
spring.datasource.url=jdbc:postgresql://host1.example.com:5432/accjtdb
spring.datasource.password=accpassword
spring.datasource.tomcat.max-active=3
spring.jpa.generate-ddl=true
spring.data.mongodb.host=mongos-host2.example.com
spring.data.mongodb.port=27017
spring.data.mongodb.database=eva_accession_sharded
spring.data.mongodb.username=mongouser
spring.data.mongodb.password=mongopassword
spring.data.mongodb.authentication-database=admin
spring.main.web-application-type=none
spring.main.allow-bean-definition-overriding=true
spring.jpa.properties.hibernate.jdbc.lob.non_contextual_creation=true
spring.batch.job.names=INGEST_REMAPPED_VARIANTS_FROM_VCF_JOB

mongodb.read-preference=secondaryPreferred

parameters.chunkSize=1000
parameters.assemblyAccession=GCA_00000002.1
parameters.vcf=
parameters.remappedFrom=GCA_00000001.1
parameters.loadTo=collection
parameters.remappingVersion=1.0
'''
        assert self.prop.get_remapping_ingestion_properties(
            source_assembly='GCA_00000001.1', target_assembly='GCA_00000002.1', load_to='collection'
        ) == expected

    def test_get_clustering_properties(self):
        expected = '''spring.datasource.driver-class-name=org.postgresql.Driver
spring.datasource.url=jdbc:postgresql://host1.example.com:5432/accjtdb
spring.datasource.password=accpassword
spring.datasource.tomcat.max-active=3
spring.jpa.generate-ddl=true
spring.data.mongodb.host=mongos-host2.example.com
spring.data.mongodb.port=27017
spring.data.mongodb.database=eva_accession_sharded
spring.data.mongodb.username=mongouser
spring.data.mongodb.password=mongopassword
spring.data.mongodb.authentication-database=admin
spring.main.web-application-type=none
spring.main.allow-bean-definition-overriding=true
spring.jpa.properties.hibernate.jdbc.lob.non_contextual_creation=true
spring.batch.job.names=CLUSTERING_RSID

mongodb.read-preference=secondaryPreferred

parameters.chunkSize=1000
parameters.assemblyAccession=GCA_00000002.1
parameters.projects=
parameters.projectAccession=
parameters.vcf=
parameters.remappedFrom=GCA_00000001.1
parameters.rsReportPath=/path/to/rs_report.txt

accessioning.instanceId=instance-1
accessioning.submitted.categoryId=ss
accessioning.clustered.categoryId=rs
accessioning.monotonic.ss.blockSize=100000
accessioning.monotonic.ss.blockStartValue=5000000000
accessioning.monotonic.ss.nextBlockInterval=1000000000
accessioning.monotonic.rs.blockSize=100000
accessioning.monotonic.rs.blockStartValue=3000000000
accessioning.monotonic.rs.nextBlockInterval=1000000000

eva.count-stats.url=https://www.ebi.ac.uk/eva/webservices/count-stats
eva.count-stats.username=statsuser
eva.count-stats.password=statspassword
'''
        assert self.prop.get_clustering_properties(
            instance=1, job_name='CLUSTERING_RSID', source_assembly='GCA_00000001.1', target_assembly='GCA_00000002.1',
            rs_report_path='/path/to/rs_report.txt') == expected

    def test_get_accessioning_properties(self):
        expected = '''spring.datasource.driver-class-name=org.postgresql.Driver
spring.datasource.url=jdbc:postgresql://host1.example.com:5432/accjtdb
spring.datasource.password=accpassword
spring.datasource.tomcat.max-active=3
spring.jpa.generate-ddl=true
spring.data.mongodb.host=mongos-host2.example.com
spring.data.mongodb.port=27017
spring.data.mongodb.database=eva_accession_sharded
spring.data.mongodb.username=mongouser
spring.data.mongodb.password=mongopassword
spring.data.mongodb.authentication-database=admin
spring.main.web-application-type=none
spring.main.allow-bean-definition-overriding=true
spring.jpa.properties.hibernate.jdbc.lob.non_contextual_creation=true
spring.batch.job.names=CREATE_SUBSNP_ACCESSION_JOB

mongodb.read-preference=secondaryPreferred

parameters.chunkSize=1000
parameters.assemblyAccession=GCA_00000001.1
parameters.assemblyReportUrl=file:/path/to/assembly_report.txt
parameters.chunkSize=100
parameters.contigNaming=NO_REPLACEMENT
parameters.fasta=/path/to/fasta.fa
parameters.forceRestart=false
parameters.projectAccession=PRJEB0001
parameters.taxonomyAccession=9906
parameters.vcfAggregation=BASIC
parameters.vcf=/path/to/vcf_file.vcf

accessioning.instanceId=instance-1
accessioning.submitted.categoryId=ss
accessioning.clustered.categoryId=rs
accessioning.monotonic.ss.blockSize=100000
accessioning.monotonic.ss.blockStartValue=5000000000
accessioning.monotonic.ss.nextBlockInterval=1000000000
accessioning.monotonic.rs.blockSize=100000
accessioning.monotonic.rs.blockStartValue=3000000000
accessioning.monotonic.rs.nextBlockInterval=1000000000

eva.count-stats.url=https://www.ebi.ac.uk/eva/webservices/count-stats
eva.count-stats.username=statsuser
eva.count-stats.password=statspassword
'''
        assert self.prop.get_accessioning_properties(
            instance=1, target_assembly='GCA_00000001.1', fasta='/path/to/fasta.fa',
            assembly_report='/path/to/assembly_report.txt', project_accession='PRJEB0001', aggregation='BASIC',
            taxonomy_accession='9906', vcf_file='/path/to/vcf_file.vcf') == expected

    def test_get_accessioning_properties_with_none(self):
        expected = '''spring.datasource.driver-class-name=org.postgresql.Driver
spring.datasource.url=jdbc:postgresql://host1.example.com:5432/accjtdb
spring.datasource.password=accpassword
spring.datasource.tomcat.max-active=3
spring.jpa.generate-ddl=true
spring.data.mongodb.host=mongos-host2.example.com
spring.data.mongodb.port=27017
spring.data.mongodb.database=eva_accession_sharded
spring.data.mongodb.username=mongouser
spring.data.mongodb.password=mongopassword
spring.data.mongodb.authentication-database=admin
spring.main.web-application-type=none
spring.main.allow-bean-definition-overriding=true
spring.jpa.properties.hibernate.jdbc.lob.non_contextual_creation=true
spring.batch.job.names=CREATE_SUBSNP_ACCESSION_JOB

mongodb.read-preference=secondaryPreferred

parameters.chunkSize=1000
parameters.assemblyAccession=GCA_00000001.1
parameters.chunkSize=100
parameters.contigNaming=NO_REPLACEMENT
parameters.forceRestart=false
parameters.projectAccession=PRJEB0001
parameters.taxonomyAccession=9906
parameters.vcfAggregation=BASIC
parameters.vcf=/path/to/vcf_file.vcf

accessioning.instanceId=instance-1
accessioning.submitted.categoryId=ss
accessioning.clustered.categoryId=rs
accessioning.monotonic.ss.blockSize=100000
accessioning.monotonic.ss.blockStartValue=5000000000
accessioning.monotonic.ss.nextBlockInterval=1000000000
accessioning.monotonic.rs.blockSize=100000
accessioning.monotonic.rs.blockStartValue=3000000000
accessioning.monotonic.rs.nextBlockInterval=1000000000

eva.count-stats.url=https://www.ebi.ac.uk/eva/webservices/count-stats
eva.count-stats.username=statsuser
eva.count-stats.password=statspassword
'''
        assert self.prop.get_accessioning_properties(
            instance=1, target_assembly='GCA_00000001.1', fasta=None,
            assembly_report=None, project_accession='PRJEB0001', aggregation='BASIC',
            taxonomy_accession='9906', vcf_file='/path/to/vcf_file.vcf') == expected

