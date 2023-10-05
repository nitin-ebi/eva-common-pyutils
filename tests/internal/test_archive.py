import os
import shutil
import tarfile
from unittest import TestCase

from ebi_eva_internal_pyutils.archive_directory import archive_directory


class TestArchive(TestCase):

    archive = os.path.join(os.path.dirname(__file__), '../resources', 'archive')

    def setUp(self) -> None:
        self.src_dir = os.path.join(self.archive, 'src')
        self.dest_dir = os.path.join(self.archive, 'dest')
        self.scratch_dir = os.path.join(self.archive, 'scratch')
        os.makedirs(self.dest_dir, exist_ok=True)
        os.makedirs(self.scratch_dir, exist_ok=True)

    def tearDown(self) -> None:
        for d in [self.dest_dir, self.scratch_dir]:
            shutil.rmtree(d)

    def test_archive_directory(self):
        archive_directory(self.src_dir, self.scratch_dir, self.dest_dir, filter_patterns=['do_not_want'])
        tar_file_path = os.path.join(self.dest_dir, 'src.tar')
        assert os.path.isfile(tar_file_path)
        with tarfile.open(tar_file_path) as tar:
            tar.extractall(path=self.scratch_dir)

            for root, dirs, files in os.walk(self.src_dir):
                for f in files:
                    relative_path = os.path.relpath(os.path.join(root, f), self.archive)
                    extracted_path = os.path.join(self.scratch_dir, relative_path)
                    if not extracted_path.endswith('gz'):
                        extracted_path = extracted_path + '.gz'
                    if 'do_not_want' not in extracted_path:
                        assert os.path.exists(extracted_path)
