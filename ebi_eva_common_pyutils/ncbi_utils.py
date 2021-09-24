import requests

from ebi_eva_common_pyutils.logger import logging_config as log_cfg


logger = log_cfg.get_logger(__name__)

eutils_url = 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/'
esearch_url = eutils_url + 'esearch.fcgi'
esummary_url = eutils_url + 'esummary.fcgi'
efetch_url = eutils_url + 'efetch.fcgi'
ensembl_url = 'http://rest.ensembl.org/info/assembly'


def get_ncbi_assembly_dicts_from_term(term):
    payload = {'db': 'Assembly', 'term': '"{}"'.format(term), 'retmode': 'JSON'}
    data = requests.get(esearch_url, params=payload).json()
    assembly_dicts = []
    if data:
        assembly_id_list = data.get('esearchresult').get('idlist')
        payload = {'db': 'Assembly', 'id': ','.join(assembly_id_list), 'retmode': 'JSON'}
        summary_list = requests.get(esummary_url, params=payload).json()
        for assembly_id in summary_list.get('result', {}).get('uids', []):
            assembly_dicts.append(summary_list.get('result').get(assembly_id))
    return assembly_dicts

