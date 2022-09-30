from unittest import TestCase
from unittest.mock import Mock, MagicMock, patch

from ebi_eva_common_pyutils.metadata_utils import resolve_variant_warehouse_db_name, get_taxonomy_code_from_taxonomy, \
    get_assembly_code_from_assembly


class TestMetadata(TestCase):

    def test_get_taxonomy_code_from_taxonomy(self):
        db_handle = MagicMock()
        with patch('ebi_eva_common_pyutils.metadata_utils.get_all_results_for_query', return_value=[('hsapiens',)]):
            taxcode = get_taxonomy_code_from_taxonomy(db_handle, 9096)
            assert taxcode == 'hsapiens'

    def test_get_assembly_code_from_assembly(self):
        db_handle = MagicMock()
        with patch('ebi_eva_common_pyutils.metadata_utils.get_all_results_for_query', return_value=[('grch38',)]):
            taxcode = get_assembly_code_from_assembly(db_handle, 'GCA_000001405.15')
            assert taxcode == 'grch38'

    def test_resolve_variant_warehouse_db_name(self):
        expected_results = [
            ('GCA_000001405.15', 9606, 'eva_hsapiens_grch38'),
            ('GCA_000263155.1', 4555, 'eva_sitalica_setariav1'),
            ('GCA_008746955.1', 8839, 'eva_aplatyrhynchos_cauwild10'),
            ('GCA_018584345.1', 3316, 'eva_tplicata_redcedarv3'),
            ('GCA_000004695.1', 352472, 'eva_ddiscoideumax4_dicty27')

        ]
        # No lookup to the database
        db_handle = MagicMock()
        for assembly, taxonomy, expected_db_name in expected_results:
            db_name = resolve_variant_warehouse_db_name(db_handle, assembly, taxonomy)
            assert db_name == expected_db_name

    def test_resolve_variant_warehouse_db_name_from_database(self):
        expected_results = [
            ('GCA_000005005.4', 4577, 'eva_maize_agpv2'),
            ('GCA_000003055.5', 30521, 'eva_bgrunniens_umd311'),
            ('GCA_900491625.1', 3329, 'eva_pabies_a541150contigsfastagz')
        ]
        # Patch the lookup functions
        ptaxonomy = patch('ebi_eva_common_pyutils.metadata_utils.get_taxonomy_code_from_taxonomy',
                          side_effect=['maize',  None,   'pabies'])
        passembly = patch('ebi_eva_common_pyutils.metadata_utils.get_assembly_code_from_assembly',
                          side_effect=['agpv2', 'umd311', None])
        with ptaxonomy, passembly:
            for assembly, taxonomy, expected_db_name in expected_results:
                db_name = resolve_variant_warehouse_db_name(None, assembly, taxonomy)
                assert db_name == expected_db_name
