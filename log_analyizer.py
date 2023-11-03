import pandas as pd

colnames=['metric', 'data_type', 'value', 'timestamp']
df = pd.read_csv("../robodatalogs/Log_9d8f62f2e34a918e.gz", quotechar='|', header=None, names=colnames)

print(df)