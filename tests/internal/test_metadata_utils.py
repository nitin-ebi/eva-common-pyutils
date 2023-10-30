from unittest import TestCase
from unittest.mock import MagicMock, patch

from ebi_eva_internal_pyutils.metadata_utils import resolve_variant_warehouse_db_name, get_taxonomy_code_from_metadata, \
    get_assembly_code_from_metadata, insert_new_assembly_and_taxonomy, build_taxonomy_code, \
    ensure_taxonomy_is_in_evapro, insert_assembly_in_evapro, update_accessioning_status, get_assembly_code


class TestMetadata(TestCase):

    def test_build_taxonomy_code(self):
        assert build_taxonomy_code('Homo sapiens') == 'hsapiens'
        assert build_taxonomy_code('Triticum dicoccoides') == 'tdicoccoides'
        assert build_taxonomy_code('Bison bonasus') == 'bbonasus'
        # Not sure if we should be raising here
        assert build_taxonomy_code('Homo') == 'h'

    def test_get_taxonomy_code_from_metadata(self):
        db_handle = MagicMock()
        with patch('ebi_eva_internal_pyutils.metadata_utils.get_all_results_for_query', return_value=[('hsapiens',)]):
            taxcode = get_taxonomy_code_from_metadata(db_handle, 9096)
            assert taxcode == 'hsapiens'

    def test_get_assembly_code(self):
        db_handle = MagicMock()
        with patch('ebi_eva_internal_pyutils.metadata_utils.get_all_results_for_query', return_value=[('',)]):
            assembly_code = get_assembly_code(db_handle, 'GCA_000001405.9')
            assert assembly_code == 'grch37'

    def test_get_assembly_code_from_metadata(self):
        db_handle = MagicMock()
        with patch('ebi_eva_internal_pyutils.metadata_utils.get_all_results_for_query', return_value=[('grch38',)]):
            assembly_code = get_assembly_code_from_metadata(db_handle, 'GCA_000001405.15')
            assert assembly_code == 'grch38'

    def test_resolve_variant_warehouse_db_name(self):
        expected_results = [
            ('GCA_000001405.1', 9606, 'eva_hsapiens_grch37'),
            # grch37p8 - patch versions should not result in a different assembly code
            ('GCA_000001405.9', 9606, 'eva_hsapiens_grch37'),
            ('GCA_000001405.15', 9606, 'eva_hsapiens_grch38'),
            # grch38p10 - patch versions should not result in a different assembly code
            ('GCA_000001405.25', 9606, 'eva_hsapiens_grch38'),
            ('GCA_000263155.1', 4555, 'eva_sitalica_setariav1'),
            ('GCA_008746955.1', 8839, 'eva_aplatyrhynchos_cauwild10'),
            ('GCA_018584345.1', 3316, 'eva_tplicata_redcedarv3'),
            ('GCA_000004695.1', 352472, 'eva_ddiscoideumax4_dicty27'),
            ('GCA_015227675.2', 10116, 'eva_rnorvegicus_mratbn72'),
            # grcm38.p1
            ('GCA_000001635.3', 10090, 'eva_mmusculus_grcm38')
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
        ptaxonomy = patch('ebi_eva_internal_pyutils.metadata_utils.get_taxonomy_code_from_metadata',
                          side_effect=['maize',  None,   'pabies'])
        passembly = patch('ebi_eva_internal_pyutils.metadata_utils.get_assembly_code_from_metadata',
                          side_effect=['agpv2', 'umd311', None])
        with ptaxonomy, passembly:
            for assembly, taxonomy, expected_db_name in expected_results:
                db_name = resolve_variant_warehouse_db_name(None, assembly, taxonomy)
                assert db_name == expected_db_name

    def test_ensure_taxonomy_is_in_evapro(self):
        db_handle = MagicMock()
        ensure_taxonomy_is_in_evapro(db_handle, 9606)
        # Checks that the taxonomy does not exist then
        db_handle.cursor().execute.assert_any_call('SELECT taxonomy_id FROM evapro.taxonomy WHERE taxonomy_id=9606')
        # Insert the taxonomy with the appropriate species name
        db_handle.cursor().execute.assert_any_call(
            'INSERT INTO evapro.taxonomy(taxonomy_id, common_name, scientific_name, taxonomy_code, eva_name) '
            'VALUES (%s, %s, %s, %s, %s)',
            (9606, 'human', 'Homo sapiens', 'hsapiens', 'human')
        )
        db_handle.reset_mock()
        # Can override the eva_name
        ensure_taxonomy_is_in_evapro(db_handle, 9606, 'tortoise')
        db_handle.cursor().execute.assert_any_call(
            'INSERT INTO evapro.taxonomy(taxonomy_id, common_name, scientific_name, taxonomy_code, eva_name) '
            'VALUES (%s, %s, %s, %s, %s)',
            (9606, 'human', 'Homo sapiens', 'hsapiens', 'tortoise')
        )
        db_handle.reset_mock()
        # Obscure taxonomy does not have a common name use scientific name instead
        ensure_taxonomy_is_in_evapro(db_handle, 665079)
        db_handle.cursor().execute.assert_any_call(
            'INSERT INTO evapro.taxonomy(taxonomy_id, common_name, scientific_name, taxonomy_code, eva_name) '
            'VALUES (%s, %s, %s, %s, %s)',
            (665079, None, 'Sclerotinia sclerotiorum 1980 UF-70', 'ssclerotiorum1980uf70', 'Sclerotinia sclerotiorum 1980 UF-70')
        )

    def test_insert_assembly_in_evapro_no_insert(self):
        db_handle = MagicMock()
        db_handle.cursor().__enter__().fetchall.return_value = (9606,)
        with patch('ebi_eva_internal_pyutils.metadata_utils.insert_taxonomy') as mock_insert:
            ensure_taxonomy_is_in_evapro(db_handle, 9606)
        mock_insert.assert_not_called()

    def test_insert_assembly_in_evapro(self):
        db_handle = MagicMock()
        with patch('ebi_eva_internal_pyutils.metadata_utils.get_all_results_for_query') as mock_results:
            # This will be used when retrieving the assembly set id after it's been inserted
            assembly_set_id = 27
            mock_results.return_value = ((assembly_set_id,),)
            insert_assembly_in_evapro(db_handle, 9606, 'GCA_000001405.15', 'GRCh38', 'grch38')
            db_handle.cursor().execute.assert_any_call(
                'INSERT INTO evapro.assembly_set(taxonomy_id, assembly_name, assembly_code) VALUES (%s, %s, %s)',
                (9606, 'GRCh38', 'grch38')
            )
            db_handle.cursor().execute.assert_any_call(
                'INSERT INTO evapro.accessioned_assembly(assembly_set_id, assembly_accession, assembly_chain, assembly_version) VALUES (%s,%s,%s,%s)',
                (assembly_set_id, 'GCA_000001405.15', 'GCA_000001405', '15')
            )

    def test_update_accessioning_status(self):
        db_handle = MagicMock()
        update_accessioning_status(db_handle, 'GCA_000001405.15', in_accessioning_flag=True)
        db_handle.cursor().execute.assert_any_call(
            ("INSERT INTO evapro.assembly_accessioning_store_status "
             "SELECT * FROM "
             "(SELECT cast('GCA_000001405.15' as text) as assembly_accession, cast('True' as boolean) as loaded) temp "
             "WHERE assembly_accession NOT IN "
             "(SELECT assembly_accession FROM evapro.assembly_accessioning_store_status)")
        )

    def test_insert_new_assembly_and_taxonomy(self):
        db_handle = MagicMock()
        with patch('ebi_eva_internal_pyutils.metadata_utils.ensure_taxonomy_is_in_evapro') as mock_tax_in_evapro, \
                patch('ebi_eva_internal_pyutils.metadata_utils.insert_assembly_in_evapro') as mock_insert_assembly, \
                patch('ebi_eva_internal_pyutils.metadata_utils.update_accessioning_status') as mock_update_status, \
                patch('ebi_eva_internal_pyutils.metadata_utils.get_all_results_for_query') as mock_get_results:
            insert_new_assembly_and_taxonomy(db_handle, 'GCA_000001405.15', 9606)
        mock_tax_in_evapro.assert_called_once_with(db_handle, 9606, None)
        mock_insert_assembly.assert_called_once_with(db_handle, 9606, 'GCA_000001405.15', 'GRCh38', 'grch38')
        mock_update_status.assert_called_once_with(db_handle, 'GCA_000001405.15', True)

    def test_no_insert_new_assembly_and_taxonomy(self):
        db_handle = MagicMock()
        with patch('ebi_eva_internal_pyutils.metadata_utils.ensure_taxonomy_is_in_evapro') as mock_tax_in_evapro, \
                patch('ebi_eva_internal_pyutils.metadata_utils.insert_assembly_in_evapro') as mock_insert_assembly, \
                patch('ebi_eva_internal_pyutils.metadata_utils.update_accessioning_status') as mock_update_status, \
                patch('ebi_eva_internal_pyutils.metadata_utils.get_all_results_for_query') as mock_get_results:
            mock_get_results.return_value = ((1,),)
            insert_new_assembly_and_taxonomy(db_handle, 'GCA_000001405.15', 9606)
        mock_tax_in_evapro.assert_not_called()
        mock_insert_assembly.assert_not_called()
        mock_update_status.assert_called_once_with(db_handle, 'GCA_000001405.15', True)
