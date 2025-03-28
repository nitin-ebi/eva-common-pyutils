import os

from tests.test_common import TestCommon

from ebi_eva_common_pyutils.spreadsheet.metadata_xlsx_utils import metadata_xlsx_version


class TestSpreadsheetUtils(TestCommon):

    def test_metadata_xlsx_version(self) -> None:
        metadata_file = os.path.join(self.resources_folder, 'metadata.xlsx')
        version = metadata_xlsx_version(metadata_file)
        assert version == '1.1.4'
        metadata_file = os.path.join(self.resources_folder, 'metadata_failed.xlsx')
        version = metadata_xlsx_version(metadata_file)
        assert version is None
