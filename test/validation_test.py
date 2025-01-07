import pandas as pd
import joblib
from sklearn.metrics import mean_absolute_error, mean_squared_error

class ValidationTest:
    def __init__(self):
        self.model = self.get_model()
        self.validation_data = self.get_validation_data()

    def get_lat_long(self, input_text, place_type):
        return self.model.predict([f"{input_text} {place_type}"])

    def get_validation_data(self):
        return pd.read_csv("test/validation_data.csv",
                                dtype={"lat": float, "lon": float},
                                na_values=["\\", "N", "NULL", "", "nan", "\\N"],
                                keep_default_na=True,
                                low_memory=False)

    def get_model(self):
        return joblib.load("models/model.pkl")

    def perform_validation(self):
        validation_df = self.get_validation_data()

        validation_df.dropna(subset=['lat', 'lon'], inplace=True)

        validation_df["predicted_lat_long"] = validation_df.apply(
            lambda row: self.get_lat_long(f"{row['nombre_lugar']}", row['tipo']), axis=1
        )
        validation_df["predicted_latitude"] = validation_df["predicted_lat_long"].apply(lambda x: x[0][0])
        validation_df["predicted_longitude"] = validation_df["predicted_lat_long"].apply(lambda x: x[0][1])

        mae_lat = mean_absolute_error(validation_df["lat"], validation_df["predicted_latitude"])
        mae_lon = mean_absolute_error(validation_df["lon"], validation_df["predicted_longitude"])
        mse_lat = mean_squared_error(validation_df["lat"], validation_df["predicted_latitude"])
        mse_lon = mean_squared_error(validation_df["lon"], validation_df["predicted_longitude"])

        return mae_lat, mae_lon, mse_lat, mse_lon

if __name__ == "__main__":
    validation_test = ValidationTest()
    mae_lat, mae_lon, mse_lat, mse_lon = validation_test.perform_validation()
    print(f"Mean Absolute Error (Latitude): {mae_lat}")
    print(f"Mean Absolute Error (Longitude): {mae_lon}")
    print(f"Mean Squared Error (Latitude): {mse_lat}")
    print(f"Mean Squared Error (Longitude): {mse_lon}")