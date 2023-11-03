import pandas as pd

colnames=['metric', 'data_type', 'value', 'timestamp']
df = pd.read_csv("../robodatalogs/Log_9d8f62f2e34a918e.gz", quotechar='|', header=None, names=colnames)

df_voltage = df[df['metric'] == '/SystemStats/5vRail/Voltage']
df_voltage['value'] = pd.to_numeric(df_voltage['value'])
print(df_voltage['value'].mean())
print(len(df_voltage.index))