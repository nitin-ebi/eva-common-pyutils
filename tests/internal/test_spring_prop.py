import os

from ebi_eva_internal_pyutils.spring_properties import SpringPropertiesGenerator
from tests.test_common import TestCommon


class TestSpringPropertiesGenerator(TestCommon):

    def setUp(self) -> None:
        private_file = os.path.join(self.resources_folder, 'private_settings.xml')
        self.prop = SpringPropertiesGenerator(maven_profile='dummy', private_settings_file=private_file)
        self.prop_no_mongo_cred = SpringPropertiesGenerator(maven_profile='dummy_no_mongo_cred', private_settings_file=private_file)

    def test_get_remapping_extraction_properties(self):
        expected = '''spring.data.mongodb.uri=mongodb://mongouser:mongop%40ssword@mongos-host1.example.com:27017,mongos-host2.example.com:27017/?retryWrites=true&authSource=admin
spring.datasource.driver-class-name=org.postgresql.Driver
spring.datasource.tomcat.max-active=3
spring.jpa.generate-ddl=true
spring.main.web-application-type=none
spring.main.allow-bean-definition-overriding=true
spring.jpa.properties.hibernate.jdbc.lob.non_contextual_creation=true
spring.jpa.properties.hibernate.temp.use_jdbc_metadata_defaults=false
spring.jpa.database-platform=org.hibernate.dialect.PostgreSQL9Dialect
spring.datasource.hikari.maximum-pool-size=2
spring.datasource.url=jdbc:postgresql://host1.example.com:5432/accjtdb
spring.datasource.username=accuser
spring.datasource.password=accpassword
spring.data.mongodb.database=eva_accession_sharded
spring.batch.job.names=EXPORT_SUBMITTED_VARIANTS_JOB

eva.count-stats.url=https://www.ebi.ac.uk/eva/webservices/count-stats
eva.count-stats.username=statsuser
eva.count-stats.password=statspassword

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
        expected = '''spring.data.mongodb.uri=mongodb://mongouser:mongop%40ssword@mongos-host1.example.com:27017,mongos-host2.example.com:27017/?retryWrites=true&authSource=admin
spring.datasource.driver-class-name=org.postgresql.Driver
spring.datasource.tomcat.max-active=3
spring.jpa.generate-ddl=true
spring.main.web-application-type=none
spring.main.allow-bean-definition-overriding=true
spring.jpa.properties.hibernate.jdbc.lob.non_contextual_creation=true
spring.jpa.properties.hibernate.temp.use_jdbc_metadata_defaults=false
spring.jpa.database-platform=org.hibernate.dialect.PostgreSQL9Dialect
spring.datasource.hikari.maximum-pool-size=2
spring.datasource.url=jdbc:postgresql://host1.example.com:5432/accjtdb
spring.datasource.username=accuser
spring.datasource.password=accpassword
spring.data.mongodb.database=eva_accession_sharded
spring.batch.job.names=INGEST_REMAPPED_VARIANTS_FROM_VCF_JOB

eva.count-stats.url=https://www.ebi.ac.uk/eva/webservices/count-stats
eva.count-stats.username=statsuser
eva.count-stats.password=statspassword

mongodb.read-preference=secondaryPreferred

parameters.chunkSize=1000
parameters.assemblyAccession=GCA_00000002.1
parameters.vcf=/path/to/remapped.vcf
parameters.remappedFrom=GCA_00000001.1
parameters.loadTo=collection
parameters.remappingVersion=1.0
'''
        assert self.prop.get_remapping_ingestion_properties(
            source_assembly='GCA_00000001.1', target_assembly='GCA_00000002.1', load_to='collection',
            vcf='/path/to/remapped.vcf'
        ) == expected

    def test_get_clustering_properties(self):
        expected = '''spring.data.mongodb.uri=mongodb://mongouser:mongop%40ssword@mongos-host1.example.com:27017,mongos-host2.example.com:27017/?retryWrites=true&authSource=admin
spring.datasource.driver-class-name=org.postgresql.Driver
spring.datasource.tomcat.max-active=3
spring.jpa.generate-ddl=true
spring.main.web-application-type=none
spring.main.allow-bean-definition-overriding=true
spring.jpa.properties.hibernate.jdbc.lob.non_contextual_creation=true
spring.jpa.properties.hibernate.temp.use_jdbc_metadata_defaults=false
spring.jpa.database-platform=org.hibernate.dialect.PostgreSQL9Dialect
spring.datasource.hikari.maximum-pool-size=2
spring.datasource.url=jdbc:postgresql://host1.example.com:5432/accjtdb
spring.datasource.username=accuser
spring.datasource.password=accpassword
spring.data.mongodb.database=eva_accession_sharded
spring.batch.job.names=CLUSTERING_RSID

eva.count-stats.url=https://www.ebi.ac.uk/eva/webservices/count-stats
eva.count-stats.username=statsuser
eva.count-stats.password=statspassword

mongodb.read-preference=primary

parameters.chunkSize=100
parameters.assemblyAccession=GCA_00000002.1
parameters.remappedFrom=
parameters.projects=
parameters.projectAccession=
parameters.vcf=
parameters.rsReportPath=/path/to/rs_report.txt
parameters.rsAccFile=/path/to/rs_report.txt
parameters.duplicateRSAccFile=/path/to/duplicate_rs_accessions.txt

accessioning.submitted.categoryId=ss
accessioning.clustered.categoryId=rs
accessioning.monotonic.ss.blockSize=100000
accessioning.monotonic.ss.blockStartValue=5000000000
accessioning.monotonic.ss.nextBlockInterval=1000000000
accessioning.monotonic.rs.blockSize=100000
accessioning.monotonic.rs.blockStartValue=3000000000
accessioning.monotonic.rs.nextBlockInterval=1000000000

recovery.cutoff.days=9999999
'''
        assert self.prop.get_clustering_properties(
            job_name='CLUSTERING_RSID', target_assembly='GCA_00000002.1',
            rs_report_path='/path/to/rs_report.txt',
            rs_acc_file='/path/to/rs_report.txt',
            duplicate_rs_acc_file='/path/to/duplicate_rs_accessions.txt') == expected

    def test_get_accessioning_properties(self):
        expected = '''spring.data.mongodb.uri=mongodb://mongouser:mongop%40ssword@mongos-host1.example.com:27017,mongos-host2.example.com:27017/?retryWrites=true&authSource=admin
spring.datasource.driver-class-name=org.postgresql.Driver
spring.datasource.tomcat.max-active=3
spring.jpa.generate-ddl=true
spring.main.web-application-type=none
spring.main.allow-bean-definition-overriding=true
spring.jpa.properties.hibernate.jdbc.lob.non_contextual_creation=true
spring.jpa.properties.hibernate.temp.use_jdbc_metadata_defaults=false
spring.jpa.database-platform=org.hibernate.dialect.PostgreSQL9Dialect
spring.datasource.hikari.maximum-pool-size=2
spring.datasource.url=jdbc:postgresql://host1.example.com:5432/accjtdb
spring.datasource.username=accuser
spring.datasource.password=accpassword
spring.data.mongodb.database=eva_accession_sharded
spring.batch.job.names=SUBSNP_ACCESSION_JOB

eva.count-stats.url=https://www.ebi.ac.uk/eva/webservices/count-stats
eva.count-stats.username=statsuser
eva.count-stats.password=statspassword

mongodb.read-preference=primary

parameters.chunkSize=100
parameters.assemblyAccession=GCA_00000001.1
parameters.assemblyReportUrl=file:/path/to/assembly_report.txt
parameters.contigNaming=NO_REPLACEMENT
parameters.fasta=/path/to/fasta.fa
parameters.forceRestart=false
parameters.projectAccession=PRJEB0001
parameters.taxonomyAccession=9906
parameters.vcfAggregation=BASIC
parameters.vcf=/path/to/vcf_file.vcf
parameters.outputVcf=/path/to/output_vcf.vcf
parameters.duplicateSSAccFile=/path/to/duplicate_ss_acc_file.vcf

accessioning.submitted.categoryId=ss
accessioning.clustered.categoryId=rs
accessioning.monotonic.ss.blockSize=100000
accessioning.monotonic.ss.blockStartValue=5000000000
accessioning.monotonic.ss.nextBlockInterval=1000000000
accessioning.monotonic.rs.blockSize=100000
accessioning.monotonic.rs.blockStartValue=3000000000
accessioning.monotonic.rs.nextBlockInterval=1000000000

recovery.cutoff.days=9999999
'''
        assert self.prop.get_accessioning_properties(
            job_name='SUBSNP_ACCESSION_JOB', target_assembly='GCA_00000001.1', fasta='/path/to/fasta.fa',
            assembly_report='/path/to/assembly_report.txt', project_accession='PRJEB0001', aggregation='BASIC',
            taxonomy_accession='9906', vcf_file='/path/to/vcf_file.vcf', output_vcf='/path/to/output_vcf.vcf',
            duplicate_ss_acc_file='/path/to/duplicate_ss_acc_file.vcf') == expected

    def test_get_variant_load_properties(self):
        expected = '''spring.data.mongodb.uri=mongodb://mongouser:mongop%40ssword@mongos-host1.example.com:27017,mongos-host2.example.com:27017/?retryWrites=true&authSource=admin
spring.datasource.driver-class-name=org.postgresql.Driver
spring.datasource.tomcat.max-active=3
spring.jpa.generate-ddl=true
spring.main.web-application-type=none
spring.main.allow-bean-definition-overriding=true
spring.jpa.properties.hibernate.jdbc.lob.non_contextual_creation=true
spring.jpa.properties.hibernate.temp.use_jdbc_metadata_defaults=false
spring.jpa.database-platform=org.hibernate.dialect.PostgreSQL9Dialect
spring.datasource.hikari.maximum-pool-size=2
spring.profiles.active=production,mongo
spring.profiles.include=variant-writer-mongo,variant-annotation-mongo
spring.data.mongodb.authentication-mechanism=SCRAM-SHA-1

eva.count-stats.url=https://www.ebi.ac.uk/eva/webservices/count-stats
eva.count-stats.username=statsuser
eva.count-stats.password=statspassword

mongodb.read-preference=secondaryPreferred

parameters.chunkSize=100

job.repository.driverClassName=org.postgresql.Driver
job.repository.url=jdbc:postgresql://host1.example.com:5432/jtdb
job.repository.username=varuser
job.repository.password=varpassword

db.collections.variants.name=variants_2_0
db.collections.files.name=files_2_0
db.collections.annotation-metadata.name=annotationMetadata_2_0
db.collections.annotations.name=annotations_2_0

app.opencga.path=/path/to/opencga
app.vep.cache.path=/path/to/vep/cache
app.vep.num-forks=4
app.vep.timeout=500

config.restartability.allow=false
config.db.read-preference=secondaryPreferred
config.chunk.size=200

logging.level.embl.ebi.variation.eva=DEBUG
logging.level.org.opencb.opencga=DEBUG
logging.level.org.springframework=INFO

annotation.overwrite=False

input.study.id=PRJEB0001
input.study.name=study_name
input.study.type=COLLECTION

output.dir=/path/to/output/dir
output.dir.annotation=/path/to/annotation/dir
output.dir.statistics=/path/to/stats/dir

statistics.skip=False
'''
        assert self.prop.get_variant_load_properties(project_accession='PRJEB0001', study_name='study_name',
            output_dir='/path/to/output/dir', annotation_dir='/path/to/annotation/dir',
            stats_dir='/path/to/stats/dir', vep_cache_path='/path/to/vep/cache',
            opencga_path='/path/to/opencga') == expected

    def test_get_variant_load_properties_no_mongo_cred(self):
        expected = '''spring.data.mongodb.uri=mongodb://mongos-host1.example.com:27017,mongos-host2.example.com:27017/?retryWrites=true&authSource=admin
spring.datasource.driver-class-name=org.postgresql.Driver
spring.datasource.tomcat.max-active=3
spring.jpa.generate-ddl=true
spring.main.web-application-type=none
spring.main.allow-bean-definition-overriding=true
spring.jpa.properties.hibernate.jdbc.lob.non_contextual_creation=true
spring.jpa.properties.hibernate.temp.use_jdbc_metadata_defaults=false
spring.jpa.database-platform=org.hibernate.dialect.PostgreSQL9Dialect
spring.datasource.hikari.maximum-pool-size=2
spring.profiles.active=production,mongo
spring.profiles.include=variant-writer-mongo,variant-annotation-mongo
spring.data.mongodb.authentication-mechanism=SCRAM-SHA-1

eva.count-stats.url=https://www.ebi.ac.uk/eva/webservices/count-stats
eva.count-stats.username=statsuser
eva.count-stats.password=statspassword

mongodb.read-preference=secondaryPreferred

parameters.chunkSize=100

job.repository.driverClassName=org.postgresql.Driver
job.repository.url=jdbc:postgresql://host1.example.com:5432/jtdb
job.repository.username=varuser
job.repository.password=varpassword

db.collections.variants.name=variants_2_0
db.collections.files.name=files_2_0
db.collections.annotation-metadata.name=annotationMetadata_2_0
db.collections.annotations.name=annotations_2_0

app.opencga.path=/path/to/opencga
app.vep.cache.path=/path/to/vep/cache
app.vep.num-forks=4
app.vep.timeout=500

config.restartability.allow=false
config.db.read-preference=secondaryPreferred
config.chunk.size=200

logging.level.embl.ebi.variation.eva=DEBUG
logging.level.org.opencb.opencga=DEBUG
logging.level.org.springframework=INFO

annotation.overwrite=False

input.study.id=PRJEB0001
input.study.name=study_name
input.study.type=COLLECTION

output.dir=/path/to/output/dir
output.dir.annotation=/path/to/annotation/dir
output.dir.statistics=/path/to/stats/dir

statistics.skip=False
'''
        assert self.prop_no_mongo_cred.get_variant_load_properties(project_accession='PRJEB0001', study_name='study_name',
                output_dir='/path/to/output/dir', annotation_dir='/path/to/annotation/dir',
                stats_dir='/path/to/stats/dir', vep_cache_path='/path/to/vep/cache',
                opencga_path='/path/to/opencga') == expected
    def test_get_accession_import_properties(self):
        expected = '''spring.data.mongodb.uri=mongodb://mongouser:mongop%40ssword@mongos-host1.example.com:27017,mongos-host2.example.com:27017/?retryWrites=true&authSource=admin
spring.datasource.driver-class-name=org.postgresql.Driver
spring.datasource.tomcat.max-active=3
spring.jpa.generate-ddl=true
spring.main.web-application-type=none
spring.main.allow-bean-definition-overriding=true
spring.jpa.properties.hibernate.jdbc.lob.non_contextual_creation=true
spring.jpa.properties.hibernate.temp.use_jdbc_metadata_defaults=false
spring.jpa.database-platform=org.hibernate.dialect.PostgreSQL9Dialect
spring.datasource.hikari.maximum-pool-size=2
spring.profiles.active=production,mongo
spring.profiles.include=variant-writer-mongo,variant-annotation-mongo
spring.data.mongodb.authentication-mechanism=SCRAM-SHA-1

eva.count-stats.url=https://www.ebi.ac.uk/eva/webservices/count-stats
eva.count-stats.username=statsuser
eva.count-stats.password=statspassword

mongodb.read-preference=secondaryPreferred

parameters.chunkSize=100

job.repository.driverClassName=org.postgresql.Driver
job.repository.url=jdbc:postgresql://host1.example.com:5432/jtdb
job.repository.username=varuser
job.repository.password=varpassword

db.collections.variants.name=variants_2_0
db.collections.files.name=files_2_0
db.collections.annotation-metadata.name=annotationMetadata_2_0
db.collections.annotations.name=annotations_2_0

app.opencga.path=/path/to/opencga

config.restartability.allow=false
config.db.read-preference=secondaryPreferred

logging.level.embl.ebi.variation.eva=DEBUG
logging.level.org.opencb.opencga=DEBUG
logging.level.org.springframework=INFO
'''
        assert self.prop.get_accession_import_properties(opencga_path='/path/to/opencga') == expected


    def test_get_accessioning_properties_with_none(self):
        expected = '''spring.data.mongodb.uri=mongodb://mongouser:mongop%40ssword@mongos-host1.example.com:27017,mongos-host2.example.com:27017/?retryWrites=true&authSource=admin
spring.datasource.driver-class-name=org.postgresql.Driver
spring.datasource.tomcat.max-active=3
spring.jpa.generate-ddl=true
spring.main.web-application-type=none
spring.main.allow-bean-definition-overriding=true
spring.jpa.properties.hibernate.jdbc.lob.non_contextual_creation=true
spring.jpa.properties.hibernate.temp.use_jdbc_metadata_defaults=false
spring.jpa.database-platform=org.hibernate.dialect.PostgreSQL9Dialect
spring.datasource.hikari.maximum-pool-size=2
spring.datasource.url=jdbc:postgresql://host1.example.com:5432/accjtdb
spring.datasource.username=accuser
spring.datasource.password=accpassword
spring.data.mongodb.database=eva_accession_sharded

eva.count-stats.url=https://www.ebi.ac.uk/eva/webservices/count-stats
eva.count-stats.username=statsuser
eva.count-stats.password=statspassword

mongodb.read-preference=primary

parameters.chunkSize=100
parameters.assemblyAccession=GCA_00000001.1
parameters.contigNaming=NO_REPLACEMENT
parameters.forceRestart=false
parameters.projectAccession=PRJEB0001
parameters.taxonomyAccession=9906
parameters.vcfAggregation=BASIC
parameters.vcf=/path/to/vcf_file.vcf
parameters.outputVcf=/path/to/output_vcf.vcf
parameters.duplicateSSAccFile=/path/to/duplicate_ss_acc_file.vcf

accessioning.submitted.categoryId=ss
accessioning.clustered.categoryId=rs
accessioning.monotonic.ss.blockSize=100000
accessioning.monotonic.ss.blockStartValue=5000000000
accessioning.monotonic.ss.nextBlockInterval=1000000000
accessioning.monotonic.rs.blockSize=100000
accessioning.monotonic.rs.blockStartValue=3000000000
accessioning.monotonic.rs.nextBlockInterval=1000000000

recovery.cutoff.days=9999999
'''
        assert self.prop.get_accessioning_properties(
            target_assembly='GCA_00000001.1', fasta=None, assembly_report=None, project_accession='PRJEB0001',
            aggregation='BASIC', taxonomy_accession='9906', vcf_file='/path/to/vcf_file.vcf',
            output_vcf='/path/to/output_vcf.vcf', duplicate_ss_acc_file='/path/to/duplicate_ss_acc_file.vcf') == expected

    def test_get_release_properties(self):
        expected = '''spring.data.mongodb.uri=mongodb://mongouser:mongop%40ssword@mongos-host1.example.com:27017,mongos-host2.example.com:27017/?retryWrites=true&authSource=admin
spring.datasource.driver-class-name=org.postgresql.Driver
spring.datasource.tomcat.max-active=3
spring.jpa.generate-ddl=true
spring.main.web-application-type=none
spring.main.allow-bean-definition-overriding=true
spring.jpa.properties.hibernate.jdbc.lob.non_contextual_creation=true
spring.jpa.properties.hibernate.temp.use_jdbc_metadata_defaults=false
spring.jpa.database-platform=org.hibernate.dialect.PostgreSQL9Dialect
spring.datasource.hikari.maximum-pool-size=2
spring.datasource.url=jdbc:postgresql://host1.example.com:5432/accjtdb
spring.datasource.username=accuser
spring.datasource.password=accpassword
spring.data.mongodb.database=eva_accession_sharded
spring.batch.job.names=RELEASE

eva.count-stats.url=https://www.ebi.ac.uk/eva/webservices/count-stats
eva.count-stats.username=statsuser
eva.count-stats.password=statspassword

mongodb.read-preference=secondaryPreferred

parameters.chunkSize=1000
parameters.assemblyAccession=GCA_00000002.1
parameters.taxonomyAccession=1234
parameters.contigNaming=INSDC
parameters.fasta=/path/to/fasta.fa
parameters.assemblyReportUrl=file:/path/to/assembly_report.txt
parameters.outputFolder=/path/to/output_folder
parameters.accessionedVcf=/path/to/output.vcf

logging.level.uk.ac.ebi.eva.accession.release=INFO
'''
        assert (self.prop.get_release_properties(
            job_name='RELEASE', assembly_accession='GCA_00000002.1', taxonomy_accession=1234, fasta='/path/to/fasta.fa',
            assembly_report='/path/to/assembly_report.txt', output_folder='/path/to/output_folder',
            contig_naming='INSDC', accessioned_vcf='/path/to/output.vcf')
        ) == expected
