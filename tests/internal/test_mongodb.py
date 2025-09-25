import os
import tempfile

import pymongo
from pymongo import WriteConcern, ReadPreference
from pymongo.read_concern import ReadConcern

from ebi_eva_common_pyutils.command_utils import run_command_with_output
from ebi_eva_internal_pyutils.mongodb.mongo_database import MongoDatabase
from ebi_eva_internal_pyutils.mongo_utils import get_mongo_connection_handle
from tests.test_common import TestCommon


class TestMongoDatabase(TestCommon):
    dump_db_name = "test_mongo_db"
    uri = "mongodb://localhost:27017/admin"
    local_mongo_handle = pymongo.MongoClient()
    config_file = os.path.join(os.path.dirname(os.path.realpath(__file__)), '../resources/test_config_file.xml')

    # Tests expect a local sharded Mongo instance
    def setUp(self) -> None:
        self.test_mongo_db = MongoDatabase(uri=self.uri, db_name=self.dump_db_name)
        self.dump_dir = os.path.join(self.resources_folder, self.dump_db_name)
        run_command_with_output("Drop target test database if it already exists...",
                                f"mongosh --eval 'db.dropDatabase()' {self.dump_db_name} ")
        run_command_with_output("Import test database...", f"mongorestore --dir {self.dump_dir}")

    def tearDown(self) -> None:
        pass

    def _restore_data_to_another_db(self):
        with tempfile.TemporaryDirectory() as tempdir:
            os.makedirs(tempdir, exist_ok=True)
            self.test_mongo_db.dump_data(tempdir)
            test_restore_db = MongoDatabase(uri=self.uri, db_name=self.test_mongo_db.db_name + "_restore")
            test_restore_db.drop()
            test_restore_db.restore_data(dump_dir=os.path.join(tempdir, self.test_mongo_db.db_name),
                                         mongorestore_args={
                                             "nsFrom": f'"{self.test_mongo_db.db_name}.*"',
                                             "nsTo": f'"{test_restore_db.db_name}.*"'})
            return test_restore_db

    def test_drop_database(self):
        self.test_mongo_db.drop()
        self.assertTrue(self.dump_db_name not in self.local_mongo_handle.list_database_names())

    def test_get_indexes(self):
        expected_index_map = {
            'annotations_2_0': {
                '_id_': {'v': 2, 'key': [('_id', 1)]},
                'xrefs.id_1': {'v': 2, 'key': [('xrefs.id', 1)], 'background': True},
                'ct.so_1': {'v': 2, 'key': [('ct.so', 1)], 'background': True}
            },
            'variants_2_0': {
                '_id_': {'v': 2, 'key': [('_id', 1)]},
                'chr_1_start_1_end_1': {'v': 2, 'key': [('chr', 1), ('start', 1), ('end', 1)], 'background': True},
                'ids_1': {'v': 2, 'key': [('ids', 1)], 'background': True},
                'files.sid_1_files.fid_1': {'v': 2, 'key': [('files.sid', 1), ('files.fid', 1)], 'background': True},
                'annot.xrefs_1': {'v': 2, 'key': [('annot.xrefs', 1)], 'background': True},
                'annot.so_1': {'v': 2, 'key': [('annot.so', 1)], 'background': True}
            },
            'files_2_0': {
                '_id_': {'v': 2, 'key': [('_id', 1)]},
                'unique_file': {'v': 2, 'key': [('sid', 1), ('fid', 1), ('fname', 1)], 'background': True, 'unique': True}
            },
            'annotationMetadata_2_0': {'_id_': {'v': 2, 'key': [('_id', 1)]}}
        }
        self.assertDictEqual(expected_index_map, self.test_mongo_db.get_indexes())

    def test_create_index_on_collections(self):
        collection_index_map = {'files_2_0': {'unique_file': {'background': True,
                                                              'key': [('sid', 1), ('fid', 1), ('fname', 1)],
                                                              'ns': 'test_mongo_db.files_2_0',
                                                              'unique': True,
                                                              'v': 2}}}
        test_restore_db = self._restore_data_to_another_db()
        test_restore_db.create_index_on_collections(collection_index_map=collection_index_map)
        test_restore_db_index_info = test_restore_db.get_indexes()
        # Check if index with the name "unique_file" is created on the collection
        self.assertTrue('files_2_0' in test_restore_db_index_info.keys())
        self.assertTrue('unique_file' in test_restore_db_index_info['files_2_0'])
        self.assertEqual([('sid', 1), ('fid', 1), ('fname', 1)],
                         test_restore_db_index_info['files_2_0']['unique_file']['key'])

    def test_shard_collections(self):
        test_restore_db = self._restore_data_to_another_db()
        collection_to_shard = "files_2_0"
        test_restore_db.shard_collections(collections_shard_key_map={"files_2_0": (["sid", "fid", "fname"], True)},
                                          collections_to_shard=[collection_to_shard])
        # Query meta-collection in the config database to check sharding status
        self.assertTrue(len(list(self.local_mongo_handle["config"]["collections"]
                                 .find({"_id": f"{test_restore_db.db_name}.{collection_to_shard}",
                                        "key": {"sid": 1, "fid": 1, "fname": 1}}))) > 0)

    def test_dump_data(self):
        with tempfile.TemporaryDirectory() as tempdir:
            self.test_mongo_db.dump_data(tempdir)
            self.assertTrue(os.path.isdir(os.path.join(tempdir, self.dump_db_name)))

    def test_archive_data(self):
        with tempfile.TemporaryDirectory() as tempdir:
            self.test_mongo_db.archive_data(tempdir, self.dump_db_name)
            self.assertTrue(os.path.isfile(os.path.join(tempdir, self.dump_db_name)))

    def test_restore_data(self):
        test_restore_db = self._restore_data_to_another_db()
        self.assertTrue(test_restore_db.db_name in self.local_mongo_handle.list_database_names())

    def test_export_import_data(self):
        org_collection_name = "variants_2_0"
        mongo_export_args = {
            "collection": org_collection_name
        }
        with tempfile.TemporaryDirectory() as tempdir:
            export_file_path = os.path.join(tempdir, self.dump_db_name)
            coll_doc_count = self.test_mongo_db.mongo_handle[self.dump_db_name][org_collection_name].count_documents({})
            self.test_mongo_db.export_data(export_file_path, mongo_export_args)
            with open(export_file_path, "r") as exported_file:
                export_doc_count = len(exported_file.readlines())
                self.assertEqual(coll_doc_count, export_doc_count)

            # import whatever we have exported into a new collection in the same database
            new_collection_name = "temp_variants_2_0"
            mongo_import_args = {
                "mode": "upsert",
                "collection": new_collection_name
            }
            self.test_mongo_db.import_data(export_file_path, mongo_import_args)
            imported_doc_count = self.test_mongo_db.mongo_handle[self.dump_db_name][
                new_collection_name].count_documents({})
            self.assertEqual(coll_doc_count, imported_doc_count)

            # delete the newly created temp collection
            self.test_mongo_db.mongo_handle[self.dump_db_name][new_collection_name].drop()

    def test_get_mongo_connection_handle_sets_defaults(self):
        conn = get_mongo_connection_handle('local', self.config_file)
        self.assertEqual(conn.write_concern, WriteConcern('majority'))
        self.assertEqual(conn.read_concern, ReadConcern('majority'))
        self.assertEqual(conn.read_preference, ReadPreference.PRIMARY)
