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

import logging
import subprocess


def run_command_with_output(command_description, command, return_process_output=False,
                            log_error_stream_to_output=False):
    process_output = ""

    logger.info("Starting process: " + command_description)
    logger.info("Running command: " + command)

    with subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=1, universal_newlines=True,
                          shell=True) as process:
        for line in iter(process.stdout.readline, ''):
            line = str(line).rstrip()
            logger.info(line)
            if return_process_output:
                process_output += line + "\n"
        for line in iter(process.stderr.readline, ''):
            line = str(line).rstrip()
            if not log_error_stream_to_output:
                logger.error(line)
            # Some lame utilities like mongodump and mongorestore output non-error messages to error stream
            # This is a workaround for that
            else:
                logger.info(line)
    if process.returncode != 0:
        logger.error(command_description + " failed! Refer to the error messages for details.")
        raise subprocess.CalledProcessError(process.returncode, process.args)
    else:
        logger.info(command_description + " - completed successfully")
    if return_process_output:
        return process_output


logger = logging.getLogger(__name__)
