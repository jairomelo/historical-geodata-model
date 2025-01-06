import os
from pathlib import Path
from tqdm import tqdm
import time
from typing import Dict, Any, Tuple
import numpy as np
import yaml

import joblib

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.ensemble import RandomForestRegressor
from sklearn.pipeline import Pipeline
from sklearn.metrics import mean_absolute_error
from sklearn.model_selection import cross_val_score
from sklearn.preprocessing import StandardScaler

import logging

import sys
sys.path.append(str(Path(__file__).parent.parent))
from initialization import create_dirs

create_dirs()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/train.log"),
        logging.StreamHandler() 
    ]
)
logger = logging.getLogger(__name__)

config_path = os.path.join(os.getcwd(), "config/model_config.yaml")

def load_config(config_path: str = config_path) -> Dict[Any, Any]:
    try:
        with open(config_path, "r") as f:
            config = yaml.safe_load(f)
        logger.info(f"Loaded configuration from {config_path}")
        return config
    except Exception as e:
        logger.error(f"Error loading configuration: {e}")
        raise e

def validate_paths(*paths: str) -> None:
    for path in paths:
        path_obj = Path(path)
        if not path_obj.parent.exists():
            path_obj.parent.mkdir(parents=True, exist_ok=True)
            logger.info(f"Created directory: {path_obj.parent}")


def train_test_split(data_path="training/data/train_test_split.pkl"):
    try:
        X_train, X_test, y_train, y_test = joblib.load(data_path)
        logger.info(f"Loaded train-test split from {data_path}")
        return X_train, X_test, y_train, y_test
    except Exception as e:
        logger.error(f"Error loading train-test split: {e}")
        raise e

def model_pipeline(config: Dict[str, Any]) -> Pipeline:
    pipeline = Pipeline([
        ("vectorizer", TfidfVectorizer(
            max_features=config["vectorizer"]["max_features"],
            stop_words=config["vectorizer"]["stop_words"]
        )),
        ("scaler", StandardScaler(with_mean=False)), 
        ("regressor", RandomForestRegressor(
            n_estimators=config["model"]["n_estimators"],
            max_depth=config["model"]["max_depth"],
            random_state=config["model"]["random_state"]
        ))
    ], verbose=True)
    return pipeline

def perform_cross_validation(pipeline, X_train, y_train, cv=5) -> Tuple[float, float]:
    try:
        logger.info(f"Starting {cv}-fold cross-validation...")
        scores = []
        
        scores = cross_val_score(pipeline, X_train, y_train, 
                               cv=cv,
                               scoring='neg_mean_absolute_error',
                               n_jobs=-1,
                               verbose=1)
        
        mean_mae = -np.mean(scores)
        std_mae = np.std(scores)
        logger.info(f"Cross-validation complete - Average MAE: {mean_mae:.4f} (+/- {std_mae:.4f})")
        return mean_mae, std_mae
    except Exception as e:
        logger.error(f"Error during cross-validation: {e}")
        raise e

def train_model(X_train, y_train, config):
    pipeline = model_pipeline(config)
    logger.info("Starting model training...")
    start_time = time.time()
    
    # Get the total steps in the pipeline
    n_steps = len(pipeline.steps)
    for i, (name, _) in enumerate(pipeline.steps, 1):
        logger.info(f"[{i}/{n_steps}] Starting {name} step...")
    
    pipeline.fit(X_train, y_train)
    
    training_time = time.time() - start_time
    logger.info(f"Model training completed in {training_time:.2f} seconds")
    return pipeline

def evaluate_model(model, X_test, y_test):
    try:
        y_pred = model.predict(X_test)
        mae = mean_absolute_error(y_test, y_pred)
        logger.info(f"Model evaluation MAE: {mae}")
        return mae
    except Exception as e:
        logger.error(f"Error evaluating model: {e}")
        raise e

def save_model(model, model_path="models/model.pkl"):
    try:
        joblib.dump(model, model_path)
        logger.info(f"Model saved to {model_path}")
    except Exception as e:
        logger.error(f"Error saving model: {e}")
        raise e

if __name__ == "__main__":
    try:
        logger.info("=== Starting Model Training Pipeline ===")
        
        logger.info("Loading configuration...")
        config = load_config()
        
        logger.info("Validating paths...")
        validate_paths(config["paths"]["train_test_split"], 
                      config["paths"]["model_output"])
        
        logger.info("Loading train-test split...")
        X_train, X_test, y_train, y_test = train_test_split(
            data_path=config["paths"]["train_test_split"]
        )
        logger.info(f"Data loaded - Training samples: {X_train.shape[0]}, "
                   f"Test samples: {X_test.shape[0]}")
        
        logger.info("Creating model pipeline...")
        pipeline = model_pipeline(config)
        
        logger.info("Starting cross-validation...")
        cv_mae, cv_std = perform_cross_validation(
            pipeline, X_train, y_train, 
            cv=config["training"]["cv_folds"]
        )
        
        logger.info("Training final model...")
        model = train_model(X_train, y_train, config)
        
        logger.info("Evaluating model on test set...")
        mae = evaluate_model(model, X_test, y_test)
        logger.info(f"Final model MAE: {mae}")
        
        logger.info("Saving model...")
        save_model(model, model_path=config["paths"]["model_output"])
        
        logger.info("=== Model Training Pipeline Completed Successfully ===")
        
    except Exception as e:
        logger.error(f"Error in main execution: {e}")
        raise e