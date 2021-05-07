import os
import shutil
from unittest.mock import Mock

from ebi_eva_common_pyutils.reference.assembly import NCBIAssembly
from ebi_eva_common_pyutils.reference.sequence import NCBISequence
from tests.test_common import TestCommon


class TestNCBISequence(TestCommon):


    def setUp(self) -> None:
        self.genome_folder = os.path.join(self.resources_folder, 'genomes')
        os.makedirs(self.genome_folder, exist_ok=True)

    def tearDown(self) -> None:
        shutil.rmtree(os.path.join(self.genome_folder))

    def test_check_genbank_accession_format(self):
        NCBISequence.check_genbank_accession_format('AJ312413.2')
        self.assertRaises(ValueError, NCBISequence.check_genbank_accession_format, 'NM_017001.2')

    def test_download_assembly_fasta_fail(self):
        sequence = NCBISequence('NM_017001.2', 'Rattus norvegicus', self.genome_folder)
        self.assertFalse(os.path.isfile(sequence.sequence_fasta_path))
        self.assertRaises(ValueError, sequence.download_contig_sequence_from_ncbi)

    def test_download_assembly_fasta(self):
        sequence = NCBISequence('NM_017001.2', 'Rattus norvegicus', self.genome_folder)
        self.assertFalse(os.path.isfile(sequence.sequence_fasta_path))
        sequence.download_contig_sequence_from_ncbi(genbank_only=False)
        self.assertTrue(os.path.isfile(sequence.sequence_fasta_path))

        sequence = NCBISequence('AJ312413.2', 'Tribolium castaneum', self.genome_folder)
        self.assertFalse(os.path.isfile(sequence.sequence_fasta_path))
        sequence.download_contig_sequence_from_ncbi(genbank_only=True)
        self.assertTrue(os.path.isfile(sequence.sequence_fasta_path))

