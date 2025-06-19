import os
from collections.abc import Iterable
from unittest import TestCase

from ebi_eva_common_pyutils.contig_alias.contig_alias import ContigAliasClient


class TestContigAliasClient(TestCase):
    resources = os.path.join(os.path.dirname(__file__), 'resources')

    def setUp(self) -> None:
        self.assembly_accession = 'GCA_000002945.2'
        self.client = ContigAliasClient()

    def test_assembly_contig_iter(self):
        iterator = self.client.assembly_contig_iter(self.assembly_accession)
        assert isinstance(iterator, Iterable)
        # print(list(iterator))
        assert [e.get('genbankSequenceName') for e in iterator] == ['MT', 'III', 'II', 'I']

    def test_assembly(self):
        assembly = self.client.assembly(self.assembly_accession)
        assert assembly == {
            'insdcAccession': 'GCA_000002945.2',
            'name': 'ASM294v2',
            'organism': 'Schizosaccharomyces pombe (fission yeast)',
            'taxid': 4896,
            'refseq': 'GCF_000002945.1',
            'md5checksum': None,
            'trunc512checksum': None,
            'genbankRefseqIdentical': True
        }

    def test_contig_iter(self):
        iterator = self.client.contig_iter('CU329670.1')
        assert isinstance(iterator, Iterable)
        contig = next(iterator)
        assert contig == {
            'genbankSequenceName': 'I',
            'enaSequenceName': 'I',
            'insdcAccession': 'CU329670.1',
            'refseq': 'NC_003424.3',
            'seqLength': 5579133,
            'ucscName': None,
            'md5checksum': 'a5bc80a74aae8fd7622290b11dbc8ab3',
            'trunc512checksum': None,
            'contigType': 'CHROMOSOME',
            'assembly': {
                'insdcAccession': 'GCA_000002945.2',
                'name': 'ASM294v2',
                'organism': 'Schizosaccharomyces pombe (fission yeast)',
                'taxid': 4896,
                'refseq': 'GCF_000002945.1',
                'md5checksum': None,
                'trunc512checksum': None,
                'genbankRefseqIdentical': True
            }
        }

