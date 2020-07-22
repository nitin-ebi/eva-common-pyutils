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

logger = logging.getLogger(__name__)


def is_port_in_use(port):
    import socket
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(('localhost', port)) == 0


def get_available_local_port(try_starting_with_port):
    for i in range(0, 20):
        port_to_try = try_starting_with_port + i
        logger.info("Attempting to forward remote mongo port to local port {0}...".format(port_to_try))
        if is_port_in_use(port_to_try):
            logger.info("Port {0} already in use...".format(port_to_try))
        else:
            return port_to_try
    logger.error("Could not forward to any local port!")


def forward_remote_port_to_local_port(remote_host: str, remote_port: int, local_port: int) -> int:
    port_forward_command = 'ssh -N -L{0}:localhost:{1} {2}'.format(remote_port, local_port, remote_host)
    logger.info("Forwarding port to local port using command: " + port_forward_command)
    proc = subprocess.Popen(port_forward_command.split(" "))
    return proc.pid
