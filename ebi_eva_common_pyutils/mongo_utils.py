# Copyright 2020 EMBL - European Bioinformatics Institute
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import pymongo
from urllib.parse import quote_plus
from ebi_eva_common_pyutils.command_utils import run_command_with_output
from ebi_eva_common_pyutils.common_utils import merge_two_dicts


class MongoConfig:
    parameters = None

    def __init__(self, **kwargs):
        self.parameters = kwargs
        if "port" not in kwargs:
            self.parameters["port"] = 27017
        if "host" not in kwargs:
            self.parameters["host"] = "localhost"


def get_mongo_connection_handle(username: str, password: str, host: str,
                                port: int = 27017, authentication_database: str = "admin") -> pymongo.MongoClient:
    mongo_connection_uri = "mongodb://{0}:{1}@{2}:{3}/{4}".format(username, quote_plus(password), host,
                                                                  port, authentication_database)
    return pymongo.MongoClient(mongo_connection_uri)


def copy_db_with_config(mongo_source_config: MongoConfig, mongo_destination_config: MongoConfig, mongodump_args: dict,
                        mongorestore_args: dict):
    copy_db(merge_two_dicts(mongo_source_config.parameters, mongodump_args),
            merge_two_dicts(mongo_destination_config.parameters, mongorestore_args))


def copy_db(mongodump_args: dict, mongorestore_args: dict):
    mongodump_args_str = " ".join(["--{0} {1}".format(key, value) for key, value in mongodump_args.items()])
    mongorestore_args_str = " ".join(["--{0} {1}".format(key, value) for key, value in mongorestore_args.items()])
    run_command_with_output("Running mongodump", "mongodump " + mongodump_args_str, log_error_stream_to_output=True)
    run_command_with_output("Running mongorestore", "mongorestore " + mongorestore_args_str,
                            log_error_stream_to_output=True)
