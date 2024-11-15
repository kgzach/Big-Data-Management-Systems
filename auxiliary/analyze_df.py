import pandas as pd

try:
    df = pd.read_csv('../vehicle_data.csv')
except FileNotFoundError:
    df = pd.read_csv('../scripts/vehicle_data.csv')

max_pos =  df["x"].max()
min_pos =  df["x"].min()

print(f"Max: {max_pos}, Min: {min_pos}")
