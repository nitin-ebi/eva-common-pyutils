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

from ftplib import FTP
import http
import re
import requests


def retrieve_genbank_equivalent_for_GCF_accession(assembly_accession):
    eutils_url = 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/'
    esearch_url = eutils_url + 'esearch.fcgi'
    esummary_url = eutils_url + 'esummary.fcgi'

    payload = {'db': 'Assembly', 'term': '"{}"'.format(assembly_accession), 'retmode': 'JSON'}
    data = requests.get(esearch_url, params=payload).json()

    assembly_id_list = data.get('esearchresult').get('idlist')
    payload = {'db': 'Assembly', 'id': ','.join(assembly_id_list), 'retmode': 'JSON'}

    summary_list = requests.get(esummary_url, params=payload).json()
    genbank_synonyms = set()
    if summary_list.get('result') is None:
        raise ValueError('No Genbank synonyms found for assembly %s ' % assembly_accession)
    for assembly_id in summary_list.get('result').get('uids'):
        assembly_info = summary_list.get('result').get(assembly_id)
        genbank_synonyms.add(assembly_info.get('synonym').get('genbank'))
    if len(genbank_synonyms) != 1:
        raise ValueError('%s Genbank synonyms found for assembly %s ' % (len(genbank_synonyms), assembly_accession))
    return genbank_synonyms.pop()


def resolve_assembly_name_to_GCA_accession(assembly_name):
    ENA_ASSEMBLY_NAME_QUERY_URL = "https://www.ebi.ac.uk/ena/portal/api/search" \
                                  "?result=assembly&query=assembly_name%3D%22{0}%22&format=json".format(assembly_name)
    response = requests.get(ENA_ASSEMBLY_NAME_QUERY_URL)
    if response.status_code == http.HTTPStatus.OK.value:
        response_json = response.json()
        if len(response_json) == 0:
            raise ValueError("Could not resolve assembly name {0} to a GCA accession!".format(assembly_name))
        elif len(response_json) > 1:
            raise ValueError("Assembly name {0} resolved to more than one GCA accession!".format(assembly_name))
        else:
            return response.json()[0]["accession"] + "." + response.json()[0]["version"]
    else:
        raise ValueError("Could not resolve assembly name {0} to a GCA accession!".format(assembly_name))


def get_assembly_report_url(assembly_accession):
    if re.match(r"^GC[F|A]_\d+\.\d+$", assembly_accession) is None:
        raise Exception('Invalid assembly accession: it has to be in the form of '
                        'GCF_XXXXXXXXX.X or GCA_XXXXXXXXX.X where X is a number')

    ftp = FTP('ftp.ncbi.nlm.nih.gov', timeout=600)
    ftp.login()

    genome_folder = 'genomes/all/' + '/'.join([assembly_accession[0:3], assembly_accession[4:7],
                                               assembly_accession[7:10], assembly_accession[10:13]]) + '/'
    ftp.cwd(genome_folder)

    all_genome_subfolders = []
    ftp.retrlines('NLST', lambda line: all_genome_subfolders.append(line))

    genome_subfolders = [folder for folder in all_genome_subfolders if assembly_accession in folder]

    if len(genome_subfolders) != 1:
        raise Exception('more than one folder matches the assembly accession: ' + str(genome_subfolders))

    ftp.cwd(genome_subfolders[0])
    genome_files = []
    ftp.retrlines('NLST', lambda line: genome_files.append(line))
    ftp.quit()

    assembly_reports = [genome_file for genome_file in genome_files if 'assembly_report' in genome_file]
    if len(assembly_reports) != 1:
        raise Exception('more than one file has "assembly_report" in its name: ' + str(assembly_reports))

    return 'ftp://' + 'ftp.ncbi.nlm.nih.gov' + '/' + genome_folder + genome_subfolders[0] + '/' + assembly_reports[0]
