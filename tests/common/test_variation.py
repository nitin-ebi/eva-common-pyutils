import unittest

from ebi_eva_common_pyutils.assembly_utils import retrieve_genbank_assembly_accessions_from_ncbi


class TestAssemblyUtils(unittest.TestCase):
    def test_retrieve_genbank_assembly_accessions_from_ncbi(self):
        info = retrieve_genbank_assembly_accessions_from_ncbi('GCA_002263795.2')
        print(info)
