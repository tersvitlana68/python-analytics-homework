import pandas as pd
data = {"city": ["Kyiv", "Lviv", "Odesa"], "sales": [1200, 950, 500]}
df = pd.DataFrame(data)
print("Продажі по містах:")
print(df)
average_sales = df["sales"].mean()
print("Середнє значення:", average_sales)
print("Це середній рівень продажів по трьох містах")
