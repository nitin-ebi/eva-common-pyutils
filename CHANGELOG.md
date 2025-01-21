
Changelog for ebi_eva_common_pyutils
===========================

## 0.6.12 (2025-01-21)
----------------------

- Added new properties in clustering job to run clustering qc duplicate rs accession job


## 0.6.11 (2024-11-15)
----------------------

- Added hikari max-pool-size connection property to the method that generate property for spring jobs


## 0.6.10 (2024-08-06)
----------------------

- Allow text to be returned from biosamples communicators 


## 0.6.9 (2024-07-29)
---------------------

- Add get methods to Contig alias client

## 0.6.8 (2024-07-12)
---------------------

- Add biosamples communicators 


## 0.6.7 (2024-05-15)
---------------------

- Remove the instance id and add required dummy cutoff day for accessioning and clustering 


## 0.6.6 (2024-04-15)
---------------------

- Allow NCBI function ot use API key


## 0.6.5 (2024-04-02)
---------------------

- Ensure that the port forwarding command is running before returning the process.


## 0.6.4 (2024-03-21)
---------------------

- Update Mongodb client to use mongosh instead of the legacy mongo.

## 0.6.3 (2024-02-22)
---------------------

- Variable log level when running a command


## 0.6.2 (2023-10-30)
---------------------

- Resolve patch assemblies to proper assembly code. See [here](https://github.com/EBIvariation/eva-common-pyutils/pull/50/files).


## 0.6.1 (2023-10-09)
---------------------

- Added functionality to update config (Writable Config)


## 0.6.0 (2023-10-05)
---------------------

- Refactor to separate functions made for internal EVA use
- Check Ensembl rapid release for supported assemblies 
- Scientific name fixes 


## 0.5.7 (2023-08-21)
---------------------

- Metadata: use scientific name when no common name is available

0.5.6 (2023-06-26)
----------------

- Fix log duplication in stdout
- SpringProperties generation: Support release automation and provide empty mongo username/password when missing

0.5.5 (2023-06-02)
----------------

- Include multiple mongos hosts during Spring properties generation

0.5.4 (2023-05-22)
----------------

- Add job tracker properties to accession import job

0.5.3 (2023-05-07)
----------------

- Add spring properties generation option for variant load and accession import jobs

0.5.2 (2023-03-03)
----------------

- Add new spring properties generation options

0.5.1 (2022-12-21)
----------------

- Script to archive directories
- Get accession database from settings in properties generator

0.5 (2022-11-15)
----------------

- Client for contig alias API
- Default read/write concerns for mongo clients
- Utilities for printing tables, generating properties files, scientific names

0.4 (2022-10-04)
----------------

- New functions to create assemblies in the metadata databases


0.3.22 (2022-07-27)
-------------------

- New option in Nextflow.join_pipeline to keep the two pipeline independent

0.3.21 (2022-05-27)
-------------------

- New function to retrieve contig alias credentials.
- New functions to search NCBI taxonomy info

0.3.20 (2022-01-26)
-------------------

- New function to get count service credentials from maven profile.


0.3.19 (2022-01-14)
-------------------

- New function to add File handler to the logging configuration.


0.3.18 (2021-10-07)
-------------------

- Bugfix in retrieval of assembly/taxonomy code.


v0.3.17 (2021-09-28)
-----------------

 - New function to retrieve the variant warehouse database name even if it does not exist already

v0.3.16 (2021-08-06)
-----------------

 - Add function to normalise species scientific name


v0.3.15 (2021-07-09)
-------------------

 - Refactor & add functionality for accessing database credentials.

