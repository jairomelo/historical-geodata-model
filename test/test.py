import pandas as pd
import joblib
import plotly.express as px

def get_lat_long(model, input_text, place_type):
    return model.predict([f"{input_text} {place_type}"])

def calculate_centroid(df):
    return df.mean(axis=0)


model = joblib.load("models/model.pkl")

try:
    df = pd.read_csv("test/testing_data.csv",
                     dtype={"latitude": float, "longitude": float},
                     na_values=["\\", "N", "NULL", "", "nan", "\\N"],  
                     keep_default_na=True, 
                     low_memory=False,
                     )
except pd.errors.ParserError as e:
    raise e


sample = df.sample(n=20)

sample["predicted_lat_long"] = sample.apply(lambda row: get_lat_long(model, f"{row['nombre_lugar']} {row['otros_nombres']}", row['tipo']), axis=1)

sample["latitud"] = sample["predicted_lat_long"].apply(lambda x: x[0][0])
sample["longitud"] = sample["predicted_lat_long"].apply(lambda x: x[0][1])

centroid = calculate_centroid(sample[["latitud", "longitud"]])

print(sample[["nombre_lugar", "otros_nombres", "tipo", "latitud", "longitud"]])

fig = px.scatter_mapbox(sample, 
                        lat="latitud", 
                        lon="longitud", 
                        color="tipo", 
                        zoom=10, 
                        center={"lat": centroid[0], "lon": centroid[1]}, 
                        mapbox_style="open-street-map", 
                        hover_data=["nombre_lugar", "otros_nombres", "tipo"])
fig.show()



def run_test():
    pass

if __name__ == "__main__":
    run_test()