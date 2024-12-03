# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved
from dataclasses import dataclass

import itertools
import logging
from pathlib import Path
from typing import Any, Iterable, List, Optional, Sequence

from hydra.types import HydraContext
from hydra.core.config_store import ConfigStore
from hydra.core.override_parser.overrides_parser import OverridesParser
from hydra.core.plugins import Plugins
from hydra.plugins.launcher import Launcher
from hydra.plugins.sweeper import Sweeper
from hydra.types import TaskFunction
from omegaconf import DictConfig, OmegaConf

# IMPORTANT:
# If your plugin imports any module that takes more than a fraction of a second to import,
# Import the module lazily (typically inside sweep()).
# Installed plugins are imported during Hydra initialization and plugins that are slow to import plugins will slow
# the startup of ALL hydra applications.
# Another approach is to place heavy includes in a file prefixed by _, such as _core.py:
# Hydra will not look for plugin in such files and will not import them during plugin discovery.

log = logging.getLogger(__name__)


@dataclass
class LauncherConfig:
    _target_: str = (
        "hydra_plugins.pbs_sweeper_plugin.pbs_sweeper.PBSSweeper"
    )
    # max number of jobs to run in the same batch.
    max_batch_size: Optional[int] = None
    foo: int = 10
    bar: str = "abcde"


ConfigStore.instance().store(group="hydra/sweeper", name="pbs", node=LauncherConfig)


class PBSSweeper(Sweeper):
    def __init__(self, max_batch_size: int, foo: str, bar: str):
        self.max_batch_size = max_batch_size
        self.config: Optional[DictConfig] = None
        self.launcher: Optional[Launcher] = None
        self.hydra_context: Optional[HydraContext] = None
        self.job_results = None
        self.foo = foo
        self.bar = bar

    def setup(
        self,
        *,
        hydra_context: HydraContext,
        task_function: TaskFunction,
        config: DictConfig,
    ) -> None:
        from pbs4py import PBS
        # TODO: Queue name and params and do dirs as well
        self.config = config
        self.hydra_context = hydra_context
        self.launcher = PBS(queue_name='common_cpuQ', ncpus_per_node=1, time=8, mem='16gb')

    def sweep(self, arguments: List[str]) -> Any:

        print(self.config.hydra.run.dir)
        
        assert self.config is not None
        log.info(f"PBSSweeper (foo={self.foo}, bar={self.bar}) sweeping")
        log.info(f"Sweep output dir : {self.config.hydra.sweep.dir}")

        # Save sweep run config in top level sweep working directory
        sweep_dir = Path(self.config.hydra.sweep.dir)
        sweep_dir.mkdir(parents=True, exist_ok=True)
        OmegaConf.save(self.config, sweep_dir / "multirun.yaml")

        parser = OverridesParser.create()
        parsed = parser.parse_overrides(arguments)

        lists = []
        for override in parsed:
            if override.is_sweep_override():
                # Sweepers must manipulate only overrides that return true to is_sweep_override()
                # This syntax is shared across all sweepers, so it may limiting.
                # Sweeper must respect this though: failing to do so will cause all sorts of hard to debug issues.
                # If you would like to propose an extension to the grammar (enabling new types of sweep overrides)
                # Please file an issue and describe the use case and the proposed syntax.
                # Be aware that syntax extensions are potentially breaking compatibility for existing users and the
                # use case will be scrutinized heavily before the syntax is changed.
                sweep_choices = override.sweep_string_iterator()
                key = override.get_key_element()
                sweep = [f"{key}={val}" for val in sweep_choices]
                lists.append(sweep)
            else:
                key = override.get_key_element()
                value = override.get_value_element_as_str()
                lists.append([f"{key}={value}"])
        batches = list(itertools.product(*lists))

        returns = []
        for idx, batch in enumerate(batches):
            self.validate_batch_is_legal([batch])
            job_script = "python src/main.py " + f"hydra.run.dir={self.config.hydra.sweep.dir}/{idx} " + ' '.join(batch)
            out = self.launcher.launch(job_name=f"hydra_job_{idx}", job_body=[job_script])
            log.info(out)
