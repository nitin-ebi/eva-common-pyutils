import os
import shutil
from ebi_eva_common_pyutils.nextflow import LinearNextFlowPipeline
from tests.test_common import TestCommon


class TestLinearNextFlowPipeline(TestCommon):
    # Tests expect a local Nextflow installation
    def setUp(self) -> None:
        self.nextflow_test_dir = os.path.join(self.resources_folder, "nextflow_tests")
        shutil.rmtree(self.nextflow_test_dir, ignore_errors=True)
        self.workflow_file_path = os.path.join(self.nextflow_test_dir, "linear_pipeline.nf")
        os.makedirs(self.nextflow_test_dir, exist_ok=True)

    def tearDown(self) -> None:
        pass

    def test_linear_pipeline_uninterrupted(self):
        pipeline_output_file = os.path.join(self.nextflow_test_dir, "linear_pipeline_output.txt")
        pipeline = LinearNextFlowPipeline(workflow_file_path=self.workflow_file_path,
                                          working_dir=self.nextflow_test_dir)
        pipeline.add_process(process_name="first_process",
                             command_to_run=f"echo first_process > {pipeline_output_file}")
        pipeline.add_process(process_name="second_process",
                             command_to_run=f"echo second_process >> {pipeline_output_file}")
        pipeline.run_pipeline()
        self.assertEqual("first_process\nsecond_process", open(pipeline_output_file).read().strip())

    def test_linear_pipeline_resumption(self):
        pipeline_output_file = os.path.join(self.nextflow_test_dir, "linear_pipeline_output.txt")
        non_existent_file = os.path.join(self.nextflow_test_dir, "non_existent_file.txt")
        pipeline = LinearNextFlowPipeline(workflow_file_path=self.workflow_file_path,
                                          working_dir=self.nextflow_test_dir)
        pipeline.add_process(process_name="first_process",
                             command_to_run=f"echo first_process >> {pipeline_output_file}")
        pipeline.add_process(process_name="second_process",
                             command_to_run=f"echo second_process >> {pipeline_output_file}")
        # This process will fail during the first run
        pipeline.add_process(process_name="third_process",
                             command_to_run=f"cat {non_existent_file} >> {pipeline_output_file}")
        pipeline.run_pipeline()
        self.assertEqual("first_process\nsecond_process", open(pipeline_output_file).read().strip())

        # Create the non-existent file
        with open(non_existent_file, "w") as non_existent_file_handle:
            non_existent_file_handle.write("third_process")
            non_existent_file_handle.flush()
        pipeline.run_pipeline(resume=True)
        # If process resumes properly it should resume from the third process
        self.assertEqual("first_process\nsecond_process\nthird_process", open(pipeline_output_file).read().strip())
