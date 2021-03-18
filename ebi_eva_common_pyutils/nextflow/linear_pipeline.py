# Copyright 2021 EMBL - European Bioinformatics Institute
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

import os
from ebi_eva_common_pyutils.logger import AppLogger


class LinearNextFlowPipeline(AppLogger):
    """
    Simple linear pipeline that supports resumption
    """
    def __init__(self, workflow_file_path, nextflow_binary_path='nextflow', nextflow_config_path=None, working_dir="."):
        self.workflow_file_path = workflow_file_path
        self.nextflow_binary_path = nextflow_binary_path
        self.nextflow_config_path = nextflow_config_path
        self.working_dir = working_dir
        # Remove pipeline file if it already exists
        if os.path.exists(workflow_file_path):
            os.remove(self.workflow_file_path)
        # Number of processes in the pipeline
        self.num_processes = 0

    def _write_to_pipeline_file(self, content):
        with open(self.workflow_file_path, "a") as pipeline_file_handle:
            pipeline_file_handle.write(content + "\n")

    def add_process(self, process_name, command_to_run, memory_in_gb=4):
        # This hack is needed to kick-off the initial process in Nextflow
        previous_process_output_flag = "true" if self.num_processes == 0 else f"flag{self.num_processes}"
        current_process_output_flag = f"flag{self.num_processes + 1}"
        process_string = f"""process {process_name} {{
                memory='{memory_in_gb} GB'
                input:
                    val flag from {previous_process_output_flag}
                output:
                    val true into {current_process_output_flag}
                script:
                \"\"\"
                {command_to_run}
                \"\"\"
            }}
            """
        self._write_to_pipeline_file(process_string)
        self.num_processes += 1

    def run_pipeline(self, resume=False):
        workflow_command = f"cd {self.working_dir} && {self.nextflow_binary_path} run {self.workflow_file_path}"
        workflow_command += f" -c {self.nextflow_config_path}" if self.nextflow_config_path else ""
        workflow_command += f" -with-report {self.workflow_file_path}.report.html"
        workflow_command += " -resume" if resume else ""
        try:
            os.system(workflow_command)
        except:
            pass
