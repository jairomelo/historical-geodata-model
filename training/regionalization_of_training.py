from shapely.geometry import Point, Polygon
import pandas as pd

americas_polygon = Polygon([
    (-179.231086, 71.439786),  
    (-56.000000, 71.439786),  
    (-56.000000, -54.000000),  
    (-179.231086, -54.000000),  
    (-179.231086, 71.439786) 
])

df = pd.read_csv("training/data/training_data.csv",
                 dtype={"latitude": float, "longitude": float},
                 na_values=["\\", "N", "NULL", "", "nan", "\\N"])

df = df[df['latitude'].notna() & df['longitude'].notna()]

df['is_in_americas'] = df.apply(
    lambda row: americas_polygon.contains(Point(row['longitude'], row['latitude'])),
    axis=1
)

places_in_americas = df[df['is_in_americas']]

places_in_americas.to_csv("training/data/training_data_americas.csv", index=False)