"""
Module that defines the abstract pipeline component class
"""
from typing import Dict
from abc import ABCMeta, abstractmethod
from configparser import ConfigParser
from logging import Logger
from pyspark.sql import SparkSession

from core.configuration import parse_configuration
from core.data_objects.data_object import DataObject
from core.log import generate_logger
from core.spark_session import generate_spark_session


class Component(metaclass=ABCMeta):
    """
    Class that models a pipeline component.
    """
    COMPONENT_ID: str = None

    def __init__(self, general_config_path: str, component_config_path: str) -> None:
        self.input_data_objects: Dict[str, DataObject] = None
        self.output_data_objects: Dict[str, DataObject] = None
        self.config: ConfigParser = parse_configuration(general_config_path, component_config_path)
        self.logger: Logger = generate_logger(self.config)
        self.spark: SparkSession = generate_spark_session(self.config)
        self.initalize_data_objects()

    @abstractmethod
    def initalize_data_objects(self):
        """
        Method that initializes the data objects associated with the component.
        """

    def read(self):
        """
        Method that performs the read operation of the input data objects of the component.
        """
        for data_object in self.input_data_objects.values():
            data_object.read()

    @abstractmethod
    def transform(self):
        """
        Method that performs the data transformations needed to set the dataframes of the output
         data objects from the input data objects.
        """

    def write(self):
        """
        Method that performs the write operation of the output data objects.
        """
        for data_object in self.output_data_objects.values():
            data_object.write()

    def execute(self):
        """_summary_
        """
        self.logger.info(f"Starting {self.COMPONENT_ID}...")
        self.read()
        self.transform()
        self.write()
        self.logger.info(f"Finished {self.COMPONENT_ID}")
