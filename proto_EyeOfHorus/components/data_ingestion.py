from proto_EyeOfHorus.entity.artifact_entity import DataIngestionArtifact
from proto_EyeOfHorus.entity.config_entity import DataIngestionConfig
from proto_EyeOfHorus.exception import HorusException
from proto_EyeOfHorus.entity.config_entity import TrainingPipelineConfig
from proto_EyeOfHorus.logger import logging
import sys,os
from pandas import DataFrame
from sklearn.model_selection import train_test_split
from proto_EyeOfHorus.data_access.source import HorusData
from proto_EyeOfHorus.utils.main_utils import read_yaml_file
from proto_EyeOfHorus.constant.training_pipeline import SCHEMA_FILE_PATH
from proto_EyeOfHorus.entity.config_entity import TrainingPipelineConfig
from proto_EyeOfHorus.constant.training_pipeline import DATA_INGESTION_TRAIN_TEST_SPLIT_RATION
from typing import List


class DataIngestion: 

    def __init__(self, data_ingestion_config:DataIngestionConfig):

        try:
             self.data_ingestion_config=data_ingestion_config
             self._schema_config = read_yaml_file(SCHEMA_FILE_PATH)

        except Exception as e:
            raise HorusException(e,sys)


    def export_data_into_feature_store(self) -> DataFrame:

        """
         Export mongo db colllection record as data frame into feature
        """
        
        try:
             logging.info("export data from mongo db to feature store")
             # Create an instance of HorusData
             horus_data_instance = HorusData()
             data_frame = horus_data_instance.export_collections_as_dataframe(collection_names=self.data_ingestion_config.collection_name)
             feature_store_file_path = self.data_ingestion_config.feature_store_file_path
             

             # create floder

             dir_path = os.path.dirname(feature_store_file_path)
             os.makedirs(dir_path,exist_ok = True)

             data_frame.to_csv(feature_store_file_path,index=False,header=True)
             return data_frame  
        except Exception as e:
            raise HorusException(e,sys)

    def split_data_as_train_test(self, dataframe: DataFrame) -> None:
        """
        Feature store dataset will be split into train and test file
        """

        try:
            train_set, test_set = train_test_split(
                dataframe, test_size=self.data_ingestion_config.train_test_split_ratio
            )

            logging.info("Performed train test split on the dataframe")

            logging.info(
                "Exited split_data_as_train_test method of Data_Ingestion class"
            )

            dir_path = os.path.dirname(self.data_ingestion_config.training_file_path)

            os.makedirs(dir_path, exist_ok=True)

            logging.info(f"Exporting train and test file path.")

            train_set.to_csv(
                self.data_ingestion_config.training_file_path, index=False, header=True
            )

            test_set.to_csv(
                self.data_ingestion_config.testing_file_path, index=False, header=True
            )

            logging.info(f"Exported train and test file path.")
        except Exception as e:
            raise HorusData(e,sys)



    def initiate_data_ingestion(self) -> DataIngestionArtifact:
        try:
            dataframe = self.export_data_into_feature_store()
            # dataframe = dataframe.drop(self._schema_config["drop_columns"],axis=1)
            self.split_data_as_train_test(dataframe=dataframe)
            data_ingestion_artifact = DataIngestionArtifact(trained_file_path=self.data_ingestion_config.training_file_path,
            test_file_path=self.data_ingestion_config.testing_file_path)
            return data_ingestion_artifact
        except Exception as e:
            raise HorusException(e,sys)

    