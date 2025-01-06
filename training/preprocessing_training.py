import pandas as pd
import joblib
from sklearn.model_selection import train_test_split

import logging

logging.basicConfig(level=logging.INFO, filename="logs/preprocessing_training.log", encoding="utf-8")
logger = logging.getLogger(__name__)

def preprocess_training_data(data_path="training/data/training_data.csv"):
    try:
        df = pd.read_csv(data_path,
                 dtype={"latitude": float, "longitude": float},
                 na_values=["\\", "N", "NULL", "", "nan", "\\N"])

        logger.info(f"Read {df.shape[0]} rows from training data")

        # drop rows where latitude or longitude is null
        df = df[df['latitude'].notna() & df['longitude'].notna()]

        logger.info(f"Dropped {df.shape[0]} rows where latitude or longitude is null")

        df = df[["place_name", "place_type", "latitude", "longitude", "alternate_names"]]

        df["alternate_names"] = df["alternate_names"].fillna("")

        logger.info(f"Filled {df.shape[0]} null alternate names")

        df["name_with_alternates"] = df["place_name"] + "|" + df["alternate_names"]

        logger.info(f"Created {df.shape[0]} name with alternates")

        df_filtered = df[["name_with_alternates", "place_type", "latitude", "longitude"]]

        logger.info(f"Filtered {df_filtered.shape[0]} rows")

        return df_filtered
    
    except Exception as e:
        logger.error(f"Error preprocessing training data: {e}")
        raise e

def reduce_dimensionality(df):
    try:
        df['text_features'] = df['name_with_alternates'] + " " + df['place_type']
        return df[['text_features', 'latitude', 'longitude']]
    except Exception as e:
        logger.error(f"Error reducing dimensionality: {e}")
        raise e

def split_data(df):
    try:
        X = df['text_features']
        y = df[['latitude', 'longitude']]
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
        
        logger.info(f"Split data into {X_train.shape[0]} training and {X_test.shape[0]} test rows")
        return X_train, X_test, y_train, y_test
    except Exception as e:
        logger.error(f"Error splitting data: {e}")
        raise e

def save_data(X_train, X_test, y_train, y_test, file_path="training/data/train_test_split.pkl"):
    try:
        # Save train-test split to file
        joblib.dump((X_train, X_test, y_train, y_test), file_path)
        logger.info(f"Train-test split saved to {file_path}")

    except Exception as e:
        logger.error(f"Error in saving data: {str(e)}")
        raise
    
if __name__ == "__main__":
    df = preprocess_training_data(data_path="training/data/training_data_americas.csv")
    df_reduced = reduce_dimensionality(df)
    X_train, X_test, y_train, y_test = split_data(df_reduced)
    save_data(X_train, X_test, y_train, y_test)
