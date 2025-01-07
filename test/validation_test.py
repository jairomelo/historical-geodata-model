import pandas as pd
import joblib
from sklearn.metrics import mean_absolute_error, mean_squared_error

def get_lat_long(model, input_text, place_type):
    return model.predict([f"{input_text} {place_type}"])

model = joblib.load("models/model.pkl")

validation_df = pd.read_csv("test/validation_data.csv",
                            dtype={"lat": float, "lon": float},
                            na_values=["\\", "N", "NULL", "", "nan", "\\N"],
                            keep_default_na=True,
                            low_memory=False)

validation_df.dropna(subset=['lat', 'lon'], inplace=True)

validation_df["predicted_lat_long"] = validation_df.apply(
    lambda row: get_lat_long(model, f"{row['nombre_lugar']}", row['tipo']), axis=1
)
validation_df["predicted_latitude"] = validation_df["predicted_lat_long"].apply(lambda x: x[0][0])
validation_df["predicted_longitude"] = validation_df["predicted_lat_long"].apply(lambda x: x[0][1])

mae_lat = mean_absolute_error(validation_df["lat"], validation_df["predicted_latitude"])
mae_lon = mean_absolute_error(validation_df["lon"], validation_df["predicted_longitude"])
mse_lat = mean_squared_error(validation_df["lat"], validation_df["predicted_latitude"])
mse_lon = mean_squared_error(validation_df["lon"], validation_df["predicted_longitude"])

print(f"Mean Absolute Error (Latitude): {mae_lat}")
print(f"Mean Absolute Error (Longitude): {mae_lon}")
print(f"Mean Squared Error (Latitude): {mse_lat}")
print(f"Mean Squared Error (Longitude): {mse_lon}")
