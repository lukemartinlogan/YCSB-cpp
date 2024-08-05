import pandas as pd
import sys

df = pd.read_csv(sys.argv[1])
grp = df.groupby('ycsbc.workload')
new_df = grp.agg({
    'ycsbc.throughput': ['median'],
}).reset_index()
new_df.to_csv(sys.argv[2], index=False)
