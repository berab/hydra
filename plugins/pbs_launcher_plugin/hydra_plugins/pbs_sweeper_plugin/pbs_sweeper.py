# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved
from dataclasses import dataclass

import os
import subprocess
import itertools
import logging
from pathlib import Path
from typing import Any, List, Optional

from hydra.types import HydraContext
from hydra.core.config_store import ConfigStore
from hydra.core.override_parser.overrides_parser import OverridesParser
from hydra.core.plugins import Plugins
from hydra.plugins.launcher import Launcher
from hydra.plugins.sweeper import Sweeper
from hydra.types import TaskFunction
from omegaconf import DictConfig, OmegaConf


log = logging.getLogger(__name__)


@dataclass
class LauncherConfig:
    _target_: str = (
        "hydra_plugins.pbs_sweeper_plugin.pbs_sweeper.PBSSweeper"
    )
    job_name: str = "hydra_pbs_job"
    queue_name: str = "common_cpuQ"
    ncpus_per_node: int = 1
    ngpus_per_node: int = 0
    queue_node_limit: int = 10
    time: int = 8
    mem: int = 16
    profile_file: str = "~/.bashrc"


ConfigStore.instance().store(group="hydra/sweeper", name="pbs", node=LauncherConfig)


class PBSSweeper(Sweeper):
    def __init__(self, job_name:str, queue_name: str, ncpus_per_node: int, ngpus_per_node: int,
                  queue_node_limit: int, time: int, mem: int, profile_file: str):
        self.config: Optional[DictConfig] = None
        self.launcher: Optional[Launcher] = None
        self.hydra_context: Optional[HydraContext] = None
        self.job_results = None
        self.job_name = job_name
        self.queue_name = queue_name
        self.ncpus_per_node = ncpus_per_node
        self.ngpus_per_node = ngpus_per_node
        self.queue_node_limit = queue_node_limit
        self.time = time
        self.mem = mem
        self.profile_file = profile_file

    def setup(
        self,
        *,
        hydra_context: HydraContext,
        task_function: TaskFunction,
        config: DictConfig,
    ) -> None:
        from .hydra_pbs import HydraPBSLauncher

        self.config = config
        self.hydra_context = hydra_context
        self.launcher = HydraPBSLauncher(queue_name = self.queue_name,
                                         ncpus_per_node = self.ncpus_per_node,
                                         ngpus_per_node = self.ngpus_per_node,
                                         queue_node_limit = self.queue_name,
                                         time = self.time,
                                         mem = f"{self.mem}gb",
                                         profile_file = self.profile_file)

    def sweep(self, arguments: List[str]) -> Any:

        assert self.config is not None
        log.info(f"PBSSweeper sweeping {self.job_name} @{self.queue_name}")

        # Save sweep run config in top level sweep working directory
        sweep_dir = Path(self.config.hydra.sweep.dir)
        sweep_dir.mkdir(parents=True, exist_ok=True)
        OmegaConf.save(self.config, sweep_dir / "multirun.yaml")

        parser = OverridesParser.create()
        parsed = parser.parse_overrides(arguments)

        lists = []
        for override in parsed:
            if override.is_sweep_override():
                sweep_choices = override.sweep_string_iterator()
                key = override.get_key_element()
                sweep = [f"{key}={val}" for val in sweep_choices]
                lists.append(sweep)
            else:
                key = override.get_key_element()
                value = override.get_value_element_as_str()
                lists.append([f"{key}={value}"])
        batches = list(itertools.product(*lists))
        self.validate_batch_is_legal(batches)

        for idx, batch in enumerate(batches):
            username, max_queue, sleep_sec = os.environ["USER"], 30, 60
            run_dir = Path(f"{self.config.hydra.sweep.dir}/{idx}")
            run_dir.mkdir(exist_ok=True)
            queue_cmd = f"qstat | grep {username} | wc -l"
            n_queue = int(subprocess.run(queue_cmd, shell=True, text=True, capture_output=True).stdout)
            while n_queue >= max_queue:
                time.sleep(sleep_sec)
                n_queue = int(subprocess.run(queue_cmd, shell=True, text=True, capture_output=True).stdout)
            swept_batch = [override for override in simpler_batch if override.split('=')[0] in sweep_keys]
            run_name, overrides = '|'.join([override.replace('=', '\=') for override in swept_batch]), ' '.join(batch)
            job_script = "python src/main.py " + f"hydra.run.dir={self.config.hydra.sweep.dir}/{idx} run_name='{run_name}' {overrides}"
            out = self.launcher.launch(run_dir=run_dir, job_name=f"{self.job_name}_{idx}", job_body=[job_script], blocking=False)
            log.info(f"{out}: {overrides}")
