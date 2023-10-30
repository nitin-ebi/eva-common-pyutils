import requests
from lxml import etree
from retry import retry


@retry(tries=3, delay=2, backoff=1.2, jitter=(1, 3))
def download_xml_from_ena(ena_url) -> etree.XML:
    """Download and parse XML from ENA"""
    try:  # catches any kind of request error, including non-20X status code
        response = requests.get(ena_url)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        raise e
    root = etree.XML(bytes(response.text, encoding='utf-8'))
    return root


def get_assembly_name_and_taxonomy_id(assembly_accession):
    xml_root = download_xml_from_ena(f'https://www.ebi.ac.uk/ena/browser/api/xml/{assembly_accession}')
    xml_assembly = xml_root.xpath('/ASSEMBLY_SET/ASSEMBLY')
    if len(xml_assembly) == 0:
        raise ValueError(f'Assembly {assembly_accession} not found in ENA')
    assembly_name = xml_assembly[0].get('alias')
    taxonomy_id = int(xml_assembly[0].xpath('TAXON/TAXON_ID')[0].text)
    return assembly_name, taxonomy_id


def get_scientific_name_and_common_name(taxonomy_id):
    xml_root = download_xml_from_ena(f'https://www.ebi.ac.uk/ena/browser/api/xml/{taxonomy_id}')
    xml_taxon = xml_root.xpath('/TAXON_SET/taxon')
    if len(xml_taxon) == 0:
        raise ValueError(f'Taxonomy {taxonomy_id} not found in ENA')
    scientific_name = xml_taxon[0].get('scientificName')
    optional_common_name = xml_taxon[0].get('commonName')
    return scientific_name, optional_common_name
