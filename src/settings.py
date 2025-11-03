from collections import UserDict
from copy import deepcopy
from jinja2 import Environment, FileSystemLoader
import logging
import os
import json
import pydash
from pydantic import ValidationError

from src.validation.gold_settings_schema import GoldSettingsModel
from src.validation.silver_settings_schema import SilverSettingsModel

class TemplateRenderError(Exception):
    def __init__(self, original_exception):
        super().__init__(f"Error when render the setting template: {str(original_exception)}")
        self.original_exception = original_exception

class SettingsManipulation:
    def __init__(self, setting, data):
        self.setting = setting
        self.data = data

    def settings_merge(self):
        filtered_json = pydash.pick(self.setting, list(self.data.keys()))
        merged_json = pydash.merge(self.data, filtered_json)
        return merged_json

class BaseSettings(UserDict):
    def __init__(self, settings=None, template_path=None):
        super().__init__()
        self.settings = deepcopy(settings)
        self.base_dir = os.path.dirname(os.path.abspath(__file__))
        self.template_dir = os.path.join(self.base_dir, './templates/')
        self.template_path = template_path
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

    def get_rendered_config(self):
        try:
            env = Environment(loader=FileSystemLoader(self.template_dir))
            template = env.get_template(self.template_path)
            self.data = template.render(settings=self.settings)
        except Exception as e:
            raise TemplateRenderError(e)

        return self.data

class SilverSettings(BaseSettings):
    def __init__(self, settings=None, template_path='silver_settings_template.j2'):
        super().__init__(settings, template_path)
        self.validate()  
        self.data = super().get_rendered_config()

    def validate(self):
        if isinstance(self.settings, dict):
            try:
                example_model_instance = SilverSettingsModel(**self.settings)
                return example_model_instance
            except ValidationError as e:
                raise e
        else:
            raise ValueError("Settings must be a dictionary")

    def get_rendered_config(self):
        logger = self.logger

        self.data = self.data.replace('"""', '"\\""')
        logger.info(f"Rendered settings: {self.data}")
        self.data = json.loads(self.data)
        manipulator = SettingsManipulation(self.settings, self.data)
        self.data = manipulator.settings_merge()
        
        # print("data: ", self.data)
        return self.data

class GoldSettings(BaseSettings):
    def __init__(self, settings=None, template_path='gold_settings_template.j2'):
        super().__init__(settings, template_path)
        self.validate()
        self.data = super().get_rendered_config()
        
    def validate(self):
        if isinstance(self.settings, dict):
            try:
                example_model_instance = GoldSettingsModel(**self.settings)
                return example_model_instance
            except ValidationError as e:
                raise e
        else:
            raise ValueError("Settings must be a dictionary")
        
    def get_rendered_config(self):
        self.data = json.loads(self.data)
        manipulator = SettingsManipulation(self.settings, self.data)
        self.data = manipulator.settings_merge()
        
        return self.data