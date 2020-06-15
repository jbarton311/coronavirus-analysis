import os
import oyaml
import logging

LOGGER = logging.getLogger(__name__)

class ConfigCorona:
    """
    This class will house all of the attributes associated with
    filepaths and any other config-adjacent attributes we need
    """
    def __init__(self, name=None):
        self.load_config()

    def load_config_file(self):
        filepath = os.path.dirname(os.path.abspath(__file__))
        config_file = os.path.join(filepath, 'config_corona.yaml')
        with open(config_file) as f:
            config = oyaml.safe_load(f)

        return config

    def load_config(self):
        config = self.load_config_file()
        for key, value in config.items():
            LOGGER.info(f"ConfigCorona load config: setting attribute for {key}")
            setattr(self, key, value)