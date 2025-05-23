# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved
import hydra
from omegaconf import OmegaConf, DictConfig
import logging


@hydra.main(version_base=None, config_path="conf", config_name="config")
def my_app(cfg: DictConfig) -> None:
    logging.info(OmegaConf.to_yaml(cfg))


if __name__ == "__main__":
    my_app()
