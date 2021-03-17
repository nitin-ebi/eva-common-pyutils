import os
import pymongo
import tempfile

from ebi_eva_common_pyutils.command_utils import run_command_with_output
from ebi_eva_common_pyutils.mongodb import MongoDatabase
from tests.test_common import TestCommon


class TestMongoDatabase(TestCommon):

    dump_db_name = "test_mongo_db"
    uri = "mongodb://localhost:27017/admin"
    local_mongo_handle = pymongo.MongoClient()

    # Tests expect a local sharded Mongo instance
    def setUp(self) -> None:
        self.test_mongo_db = MongoDatabase(uri=self.uri, db_name=self.dump_db_name,
                                           password_required=False)
        self.dump_dir = os.path.join(self.resources_folder, self.dump_db_name)
        run_command_with_output("Drop target test database if it already exists...", f"mongo {self.dump_db_name} "
                                                                                     f"--eval 'db.dropDatabase()'")
        run_command_with_output("Import test database...", f"mongorestore --dir {self.dump_dir}")

    def tearDown(self) -> None:
        pass

    def _restore_data_to_another_db(self):
        tempdir = tempfile.TemporaryDirectory()
        self.test_mongo_db.dump_data(tempdir.name)

        test_restore_db = MongoDatabase(uri=self.uri, db_name=self.test_mongo_db.db_name + "_restore",
                                        password_required=False)
        test_restore_db.drop()
        test_restore_db.restore_data(dump_dir=tempdir.name,
                                     mongorestore_args={"nsFrom": f'"{self.test_mongo_db.db_name}.*"',
                                                        "nsTo": f'"{test_restore_db.db_name}.*"'})
        return test_restore_db

    def test_drop_database(self):
        self.test_mongo_db.drop()
        self.assertTrue(self.dump_db_name not in self.local_mongo_handle.list_database_names())

    def test_get_indexes(self):
        expected_index_map = {'annotationMetadata_2_0': {'_id_': {'key': [('_id', 1)],
                                                                  'ns': 'test_mongo_db.annotationMetadata_2_0',
                                                                  'v': 2}},
                              'annotations_2_0': {'_id_': {'key': [('_id', 1)],
                                                           'ns': 'test_mongo_db.annotations_2_0',
                                                           'v': 2},
                                                  'ct.so_1': {'background': True,
                                                              'key': [('ct.so', 1)],
                                                              'ns': 'test_mongo_db.annotations_2_0',
                                                              'v': 2},
                                                  'xrefs.id_1': {'background': True,
                                                                 'key': [('xrefs.id', 1)],
                                                                 'ns': 'test_mongo_db.annotations_2_0',
                                                                 'v': 2}},
                              'files_2_0': {'_id_': {'key': [('_id', 1)],
                                                     'ns': 'test_mongo_db.files_2_0',
                                                     'v': 2},
                                            'unique_file': {'background': True,
                                                            'key': [('sid', 1), ('fid', 1), ('fname', 1)],
                                                            'ns': 'test_mongo_db.files_2_0',
                                                            'unique': True,
                                                            'v': 2}},
                              'variants_2_0': {'_id_': {'key': [('_id', 1)],
                                                        'ns': 'test_mongo_db.variants_2_0',
                                                        'v': 2},
                                               'annot.so_1': {'background': True,
                                                              'key': [('annot.so', 1)],
                                                              'ns': 'test_mongo_db.variants_2_0',
                                                              'v': 2},
                                               'annot.xrefs_1': {'background': True,
                                                                 'key': [('annot.xrefs', 1)],
                                                                 'ns': 'test_mongo_db.variants_2_0',
                                                                 'v': 2},
                                               'chr_1_start_1_end_1': {'background': True,
                                                                       'key': [('chr', 1),
                                                                               ('start', 1),
                                                                               ('end', 1)],
                                                                       'ns': 'test_mongo_db.variants_2_0',
                                                                       'v': 2},
                                               'files.sid_1_files.fid_1': {'background': True,
                                                                           'key': [('files.sid', 1),
                                                                                   ('files.fid', 1)],
                                                                           'ns': 'test_mongo_db.variants_2_0',
                                                                           'v': 2},
                                               'ids_1': {'background': True,
                                                         'key': [('ids', 1)],
                                                         'ns': 'test_mongo_db.variants_2_0',
                                                         'v': 2}}}
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

    def test_enable_sharding(self):
        self.test_mongo_db.enable_sharding()
        # Query meta-collection in the config database to check sharding status
        self.assertTrue(len(list(self.local_mongo_handle["config"]["databases"]
                                 .find({"_id": self.test_mongo_db.db_name, "partitioned": True}))) > 0)

    def test_shard_collections(self):
        test_restore_db = self._restore_data_to_another_db()
        collection_to_shard = "files_2_0"
        test_restore_db.enable_sharding()
        test_restore_db.shard_collections(collections_shard_key_map={"files_2_0": (["sid", "fid", "fname"], True)},
                                          collections_to_shard=[collection_to_shard])
        # Query meta-collection in the config database to check sharding status
        self.assertTrue(len(list(self.local_mongo_handle["config"]["collections"]
                                 .find({"_id": f"{test_restore_db.db_name}.{collection_to_shard}",
                                        "key": {"sid": 1, "fid": 1, "fname": 1}}))) > 0)

    def test_dump_data(self):
        tempdir = tempfile.TemporaryDirectory()
        self.test_mongo_db.dump_data(tempdir.name)
        self.assertTrue(os.path.isdir(os.path.join(tempdir.name, self.dump_db_name)))

    def test_restore_data(self):
        test_restore_db = self._restore_data_to_another_db()
        self.assertTrue(test_restore_db.db_name in self.local_mongo_handle.list_database_names())
