import os
from pbs4py import PBS
from pathlib import Path
from typing import List

class HydraPBSLauncher(PBS):
    def __init__(
        self,
        queue_name: str = "common_cpuQ",
        ncpus_per_node: int = 1,
        ngpus_per_node: int = 0,
        queue_node_limit: int = 10,
        time: int = 8,
        mem: str = "16gb",
        profile_file: str = "~/.bashrc",
    ):
        super().__init__(queue_name = queue_name,
                         ncpus_per_node = ncpus_per_node,
                         ngpus_per_node = ngpus_per_node,
                         queue_node_limit = queue_node_limit,
                         time = time,
                         mem = mem,
                         profile_file = profile_file)

    def launch(self, run_dir: str, job_name: str, job_body: List[str],
               blocking: bool = True, dependency: str = None) -> str:
        filename = f'{job_name}.{self.batch_file_extension}'
        self.write_job_file(run_dir, filename, job_name, job_body, dependency) 
        return self._run_job(run_dir, filename, blocking)

    def write_job_file(self, run_dir: str, job_filename: str, job_name: str,
                       job_body: List[str], dependency: str = None):
        with open(f"{run_dir}/{job_filename}", mode='w') as fh:
            header = self._create_header(run_dir, job_name, dependency)
            for line in header:
                fh.write(line + '\n')

            for _ in range(2):
                fh.write('\n')

            fh.write(f'cd {self.workdir_env_variable}\n')
            if len(self.profile_filename) > 0:
                fh.write(f'source {self.profile_filename}\n')

            for _ in range(1):
                fh.write('\n')

            for line in job_body:
                fh.write(line + '\n')

    def _create_header(self, run_dir: str, job_name: str, dependency: str = None) -> List[str]:
        header = self._create_list_of_standard_header_options(run_dir, job_name)
        header.extend(self._create_list_of_optional_header_lines(dependency))
        return header

    def _create_list_of_standard_header_options(self, run_dir: str, job_name: str) -> List[str]:
        header_lines = [
            self._create_hashbang(),
            self._create_job_line_of_header(job_name),
            self._create_queue_line_of_header(),
            self._create_select_line_of_header(),
            self._create_walltime_line_of_header(),
            self._create_log_name_line_of_header(run_dir, job_name),
            self._create_header_line_to_join_standard_and_error_output(),
            self._create_header_line_to_set_that_job_is_not_rerunnable(),
        ]
        return header_lines

    def _create_log_name_line_of_header(self, run_dir: str, job_name: str) -> str:
        return f"#PBS -o {run_dir}/{job_name}_pbs.log"

    def _run_job(self, run_dir: str, job_filename: str, blocking: bool) -> str:
        options = ""
        if blocking:
            options += "-Wblock=true"
        command_output = os.popen(f"qsub {options} {run_dir}/{job_filename}").read().strip()
        return command_output
