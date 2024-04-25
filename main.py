
from proto_EyeOfHorus.config.mongo_db_connection import MongoDBClient
from proto_EyeOfHorus.exception import HorusException
import os,sys
from proto_EyeOfHorus.logger import logging
from proto_EyeOfHorus.pipeline.training_pipeline import TrainPipeline
import os
from proto_EyeOfHorus.utils.main_utils import read_yaml_file
from proto_EyeOfHorus.constant.training_pipeline import SAVED_MODEL_DIR

from proto_EyeOfHorus.constant.training_pipeline import TARGET_COLUMN
import matplotlib.pyplot as plt
import io
import seaborn as sns
sns.set_style('whitegrid')
plt.style.use("fivethirtyeight")


env_file_path=os.path.join(os.getcwd(),"env.yaml")

def set_env_variable(env_file_path):

    if os.getenv('MONGO_DB_URL',None) is None:
        env_config = read_yaml_file(env_file_path)
        os.environ['MONGO_DB_URL']=env_config['MONGO_DB_URL']

def main():
        try:
            set_env_variable(env_file_path)
            training_pipeline = TrainPipeline()
            training_pipeline.run_pipeline()
        except Exception as e:
            print(e)
            logging.exception(e)

if __name__=="__main__":

    main()