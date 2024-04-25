import sys
from typing import Optional
import numpy as np
import pandas as pd
import json
from proto_EyeOfHorus.config.mongo_db_connection import MongoDBClient
from proto_EyeOfHorus.constant.database import DATABASE_NAME
from proto_EyeOfHorus.exception import HorusException
from typing import List



class HorusData:
    """
    This class help to export entire mongo db record as pandas dataframe
    """

    def __init__(self):
        """
        """
        try:
            self.mongo_client = MongoDBClient(database_name=DATABASE_NAME)

        except Exception as e:
            raise HorusException(e, sys)


    def save_csv_file(self,file_path ,collection_name: str, database_name: Optional[str] = None):
        try:
            data_frame=pd.read_csv(file_path)
            data_frame.reset_index(drop=True, inplace=True)
            records = list(json.loads(data_frame.T.to_json()).values())
            if database_name is None:
                collection = self.mongo_client.database[collection_name]
            else:
                collection = self.mongo_client[database_name][collection_name]
            collection.insert_many(records)
            return len(records)
        except Exception as e:
            raise HorusException(e, sys)

    def process_row(self, row):
        """
        Process each row of DataFrame.
        """
        return {
            "metadata_cam_id": row["metadata"]["cam_id"],
            "metadata_loc_id": row["metadata"]["loc_id"],
            "metadata_pres_timestamp": row["metadata"]["pres_timestamp"],
            "persons_age": row["persons"][0]["age"] if row["persons"] else None,
            "persons_gender": row["persons"][0]["gender"] if row["persons"] else None,
        }

    def export_collections_as_dataframe(
        self, collection_names: List[str], database_name: Optional[str] = None
    ) -> pd.DataFrame:
        try:
            dfs = []  # List to store dataframes of each collection
            for collection_name in collection_names:
                if database_name is None:
                    collection = self.mongo_client.database[collection_name]
                else:
                    collection = self.mongo_client[database_name][collection_name]
                df = pd.DataFrame(list(collection.find()))

                # Process DataFrame
                processed_data = df.apply(self.process_row, axis=1)
                filtered_df = pd.DataFrame(processed_data.tolist())

                # Fill missing columns with NaN or None
                expected_columns = [
                    "metadata_cam_id",
                    "metadata_loc_id",
                    "metadata_pres_timestamp",
                    "persons_age",
                    "persons_gender",
                ]
                for column in expected_columns:
                    if column not in filtered_df.columns:
                        filtered_df[column] = np.nan if column.startswith("persons") else None

                # Drop "_id" column if present
                if "_id" in filtered_df.columns:
                    filtered_df = filtered_df.drop(columns=["_id"], axis=1)

                dfs.append(filtered_df)

            combined_df = pd.concat(dfs, ignore_index=True)
            return combined_df

        except Exception as e:
            raise HorusException(e, sys)
    
            

