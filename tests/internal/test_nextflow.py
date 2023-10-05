import os
import shutil
from ebi_eva_internal_pyutils.nextflow import LinearNextFlowPipeline, NextFlowPipeline, NextFlowProcess
from tests.test_common import TestCommon


class TestNextFlowPipeline(TestCommon):
    # Tests expect a local Nextflow installation
    def setUp(self) -> None:
        self.nextflow_test_dir = os.path.join(self.resources_folder, "nextflow_tests")
        self.nextflow_config_file = os.path.join(self.resources_folder, "nextflow.config")
        shutil.rmtree(self.nextflow_test_dir, ignore_errors=True)
        self.linear_workflow_file_path = os.path.join(self.nextflow_test_dir, "linear_pipeline.nf")
        self.non_linear_workflow_file_path = os.path.join(self.nextflow_test_dir, "non_linear_pipeline.nf")
        os.makedirs(self.nextflow_test_dir, exist_ok=True)

    def tearDown(self) -> None:
        pass

    def test_linear_pipeline_uninterrupted(self):
        pipeline_output_file = os.path.join(self.nextflow_test_dir, "linear_pipeline_output.txt")
        pipeline = LinearNextFlowPipeline()
        pipeline.add_process(process_name="first_process",
                             command_to_run=f"echo first_process > {pipeline_output_file}")
        pipeline.add_process(process_name="second_process",
                             command_to_run=f"echo second_process >> {pipeline_output_file}")
        pipeline.run_pipeline(workflow_file_path=self.linear_workflow_file_path, working_dir=self.nextflow_test_dir,
                              nextflow_config_path=self.nextflow_config_file)
        self.assertEqual("first_process\nsecond_process", open(pipeline_output_file).read().strip())

    def test_linear_pipeline_resumption(self):
        pipeline_output_file = os.path.join(self.nextflow_test_dir, "linear_pipeline_output.txt")
        non_existent_file = os.path.join(self.nextflow_test_dir, "non_existent_file.txt")
        pipeline = LinearNextFlowPipeline()
        pipeline.add_process(process_name="first_process",
                             command_to_run=f"echo first_process > {pipeline_output_file}")
        pipeline.add_process(process_name="second_process",
                             command_to_run=f"echo second_process >> {pipeline_output_file}")
        # This process will fail during the first run
        pipeline.add_process(process_name="third_process",
                             command_to_run=f"cat {non_existent_file} >> {pipeline_output_file}")
        try:
            pipeline.run_pipeline(workflow_file_path=self.linear_workflow_file_path,
                                  working_dir=self.nextflow_test_dir, nextflow_config_path=self.nextflow_config_file)
        except:
            pass
        self.assertEqual("first_process\nsecond_process", open(pipeline_output_file).read().strip())

        # Create the non-existent file
        with open(non_existent_file, "w") as non_existent_file_handle:
            non_existent_file_handle.write("third_process")
            non_existent_file_handle.flush()
        pipeline.run_pipeline(workflow_file_path=self.linear_workflow_file_path, working_dir=self.nextflow_test_dir,
                              resume=True, nextflow_config_path=self.nextflow_config_file)
        # If the pipeline resumes properly it should resume from the third process
        self.assertEqual("first_process\nsecond_process\nthird_process", open(pipeline_output_file).read().strip())

    def test_non_linear_pipeline_uninterrupted(self):
        pipeline_output_file = os.path.join(self.nextflow_test_dir, "non_linear_pipeline_output.txt")
        p1 = NextFlowProcess(process_name="first_process",
                             command_to_run=f"echo first_process > {pipeline_output_file}")
        p2 = NextFlowProcess(process_name="second_process",
                             command_to_run=f"echo second_process >> {pipeline_output_file}")
        p3 = NextFlowProcess(process_name="third_process",
                             command_to_run=f"echo third_process >> {pipeline_output_file}")
        p4 = NextFlowProcess(process_name="fourth_process",
                             command_to_run=f"echo fourth_process >> {pipeline_output_file}")
        # Dependency graph
        # p2 and p3 rely on p1 to finish
        # p4 relies on p2 and p3 to finish
        #    p1
        #  /    \
        # p2    p3
        #  \   /
        #   p4
        pipeline = NextFlowPipeline(process_dependency_map={p4: [p2, p3], p2: [p1], p3: [p1]})
        pipeline.run_pipeline(workflow_file_path=self.non_linear_workflow_file_path, working_dir=self.nextflow_test_dir,
                              nextflow_config_path=self.nextflow_config_file)
        lines = [line.strip() for line in open(pipeline_output_file).readlines()]
        self.assertEqual("first_process", lines[0])
        # Due to parallelism, order in which p2 and p3 are executed cannot be guaranteed
        self.assertTrue("second_process" in lines[1:3] and "third_process" in lines[1:3])
        self.assertTrue("fourth_process", lines[3])

    def test_non_linear_pipeline_resumption(self):
        pipeline_output_file = os.path.join(self.nextflow_test_dir, "non_linear_pipeline_output.txt")
        p1 = NextFlowProcess(process_name="first_process",
                             command_to_run=f"echo first_process > {pipeline_output_file}")
        p2 = NextFlowProcess(process_name="second_process",
                             command_to_run=f"echo second_process >> {pipeline_output_file}")
        non_existent_file = os.path.join(self.nextflow_test_dir, "non_existent_file.txt")
        # This process will fail during the initial run
        p3 = NextFlowProcess(process_name="third_process",
                             command_to_run=f"cat {non_existent_file} >> {pipeline_output_file}")
        # This process won't be started during the initial run because of its dependency p3 failed above
        p4 = NextFlowProcess(process_name="fourth_process",
                             command_to_run=f"echo fourth_process >> {pipeline_output_file}")
        # Dependency graph
        #    p1
        #  /    \
        # p2    p3
        #  \   /
        #   p4
        pipeline = NextFlowPipeline(process_dependency_map={p4: [p2, p3], p2: [p1], p3: [p1]})
        try:
            pipeline.run_pipeline(workflow_file_path=self.non_linear_workflow_file_path,
                                  working_dir=self.nextflow_test_dir, nextflow_config_path=self.nextflow_config_file)
        except:
            pass
        lines = [line.strip() for line in open(pipeline_output_file).readlines()]
        self.assertEqual(2, len(lines))
        self.assertEqual("first_process", lines[0])
        self.assertEqual("second_process", lines[1])

        # Create the non-existent file
        with open(non_existent_file, "w") as non_existent_file_handle:
            non_existent_file_handle.write("third_process\n")
            non_existent_file_handle.flush()
        pipeline.run_pipeline(workflow_file_path=self.non_linear_workflow_file_path, working_dir=self.nextflow_test_dir,
                              resume=True, nextflow_config_path=self.nextflow_config_file)
        # If the pipeline resumes properly it should resume from the fourth process
        lines = [line.strip() for line in open(pipeline_output_file).readlines()]
        self.assertEqual(4, len(lines))
        self.assertEqual("first_process", lines[0])
        # Due to resumption, p2 which was already executed won't be executed this time around
        # p3 and p4 will be executed in the resumed flow
        self.assertTrue("second_process", lines[1])
        self.assertTrue("third_process", lines[2])
        self.assertTrue("fourth_process", lines[3])

    def test_non_linear_pipeline_join(self):
        pipeline_output_file = os.path.join(self.nextflow_test_dir, "non_linear_pipeline_output.txt")
        p1 = NextFlowProcess(process_name="first_process",
                             command_to_run=f"echo first_process > {pipeline_output_file}")
        p2 = NextFlowProcess(process_name="second_process",
                             command_to_run=f"echo second_process >> {pipeline_output_file}")
        p3 = NextFlowProcess(process_name="third_process",
                             command_to_run=f"echo third_process >> {pipeline_output_file}")
        p4 = NextFlowProcess(process_name="fourth_process",
                             command_to_run=f"echo fourth_process >> {pipeline_output_file}")
        p5 = NextFlowProcess(process_name="fifth_process",
                             command_to_run=f"echo fifth_process >> {pipeline_output_file}")
        p6 = NextFlowProcess(process_name="sixth_process",
                             command_to_run=f"echo sixth_process >> {pipeline_output_file}")
        # Dependency graphs
        #   p1           p4   p5         p1
        # /    \     +	  \  /    =     /  \
        # p2    p3          p6	       p2  p3
        #                              /\  /\
        #                              | \/ |
        #                              p4  p5
        #                              \   /
        #                                p6
        pipe1 = NextFlowPipeline(process_dependency_map={p2: [p1], p3: [p1]})
        pipe2 = NextFlowPipeline(process_dependency_map={p6: [p4, p5]})
        pipe3 = NextFlowPipeline.join_pipelines(pipe1, pipe2)
        pipe3.run_pipeline(workflow_file_path=self.non_linear_workflow_file_path, working_dir=self.nextflow_test_dir,
                           nextflow_config_path=self.nextflow_config_file)

        lines = [line.strip() for line in open(pipeline_output_file).readlines()]
        self.assertEqual(6, len(lines))
        self.assertEqual("first_process", lines[0])
        # Due to parallelism, order in which p2 and p3 are executed cannot be guaranteed
        self.assertTrue("second_process" in lines[1:3] and "third_process" in lines[1:3])
        # Due to parallelism, order in which p4 and p5 are executed cannot be guaranteed
        self.assertTrue("fourth_process" in lines[3:5] and "fifth_process" in lines[3:5])
        self.assertEqual("sixth_process", lines[5])

    def test_pipeline_join_no_dependencies(self):
        pipeline_output_file = os.path.join(self.nextflow_test_dir, "non_linear_pipeline_output.txt")
        if os.path.isfile(pipeline_output_file):
            os.remove(pipeline_output_file)

        p1 = NextFlowProcess(process_name="first_process",
                             command_to_run=f"echo first_process >> {pipeline_output_file}")
        p2 = NextFlowProcess(process_name="second_process",
                             command_to_run=f"echo second_process >> {pipeline_output_file}")
        p3 = NextFlowProcess(process_name="third_process",
                             command_to_run=f"echo third_process >> {pipeline_output_file}")
        p4 = NextFlowProcess(process_name="fourth_process",
                             command_to_run=f"echo fourth_process >> {pipeline_output_file}")
        p5 = NextFlowProcess(process_name="fifth_process",
                             command_to_run=f"echo fifth_process >> {pipeline_output_file}")
        # Dependency graphs
        #   p1	         p1   p4         -p1--    p4
        # /    \     +	  \  /    =     /  \  \  /
        # p2    p3         p5          p2  p3  p5
        #
        pipe1 = NextFlowPipeline(process_dependency_map={p2: [p1], p3: [p1]})
        pipe2 = NextFlowPipeline(process_dependency_map={p5: [p1, p4]})
        pipe3 = NextFlowPipeline.join_pipelines(pipe1, pipe2, with_dependencies=False)
        pipe3.run_pipeline(workflow_file_path=self.non_linear_workflow_file_path, working_dir=self.nextflow_test_dir,
                           nextflow_config_path=self.nextflow_config_file)
        lines = [line.strip() for line in open(pipeline_output_file).readlines()]
        self.assertEqual(5, len(lines))
        # Due to parallelism, order in which p1 and p4 are executed cannot be guaranteed
        self.assertTrue("first_process" in lines[0:2] and "fourth_process" in lines[0:2])
        # Due to parallelism, order in which p2, p3 and p5 are executed cannot be guaranteed
        self.assertTrue("second_process" in lines[2:5] and "third_process" in lines[2:5] and
                        "fifth_process" in lines[2:5])
