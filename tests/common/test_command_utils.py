import logging
import os
import shutil
import ebi_eva_common_pyutils.command_utils
from ebi_eva_common_pyutils import command_utils
from ebi_eva_common_pyutils.command_utils import run_command_with_output
from tests.test_common import TestCommon


def touch(name):
    open(name, 'w').close()


class TestCommand(TestCommon):

    def setUp(self) -> None:
        # Create a directory with files
        self.test_dir = os.path.join(self.resources_folder, 'test_commmands')
        os.makedirs(self.test_dir)
        for i in range(1, 10):
            touch(os.path.join(self.test_dir, f'file_{i}'))

    def tearDown(self) -> None:
        shutil.rmtree(self.test_dir)

    def test_run_command_with_output(self):
        expected_output = 'file_1\nfile_2\nfile_3\nfile_4\nfile_5\nfile_6\nfile_7\nfile_8\nfile_9\n'
        content = run_command_with_output('Run list command', f'ls {self.test_dir}', return_process_output=True)
        assert expected_output == content

    def test_run_command_without_output_default_log(self):
        expected_output = [
            'Starting process: Run list command',
            f'Running command: ls {self.test_dir}',
            'file_1', 'file_2', 'file_3', 'file_4', 'file_5', 'file_6', 'file_7', 'file_8', 'file_9',
            'Run list command - completed successfully'
        ]

        with self.assertLogs(command_utils.__name__, level=logging.DEBUG) as lc:
            run_command_with_output('Run list command', f'ls {self.test_dir}')
            assert lc.output == ['INFO:ebi_eva_common_pyutils.command_utils:' + e for e in expected_output]

    def test_run_command_without_output_debug_log(self):
        expected_output = [
            'Starting process: Run list command',
            f'Running command: ls {self.test_dir}',
            'file_1', 'file_2', 'file_3', 'file_4', 'file_5', 'file_6', 'file_7', 'file_8', 'file_9',
            'Run list command - completed successfully'
        ]

        with self.assertLogs(command_utils.__name__, level=logging.DEBUG) as lc:
            run_command_with_output('Run list command', f'ls {self.test_dir}', stderr_log_level=logging.DEBUG,
                                    stdout_log_level=logging.DEBUG)
            assert lc.output == ['DEBUG:ebi_eva_common_pyutils.command_utils:' + e for e in expected_output]


