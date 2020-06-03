import pandas as pd


# We're going to be calculating memory usage a lot,
# so we'll create a function to save us some time!
def mem_usage(pandas_obj):
    if isinstance(pandas_obj,pd.DataFrame):
        usage_b = pandas_obj.memory_usage(deep=True).sum()
    else: # we assume if not a df it's a series
        usage_b = pandas_obj.memory_usage(deep=True)
    usage_mb = usage_b / 1024 ** 2 # convert bytes to megabytes
    return "{:03.2f} MB".format(usage_mb)

def downcast(df):
    df_int = df.select_dtypes(include=['int'])
    converted = df.copy()
    converted[df_int.columns] = df[df_int.columns].apply(pd.to_numeric, downcast='unsigned')

    compare = pd.concat([df_int.dtypes, converted.dtypes], axis=1)
    compare.columns = ['before', 'after']
    compare.apply(pd.Series.value_counts)

    df_obj = df.select_dtypes(include=['object'])
    colunas = df_obj.columns
    mapa = dict()

    for coluna in colunas:
        mapa[coluna] = 'category'

    converted = converted.astype(mapa)
    print(mem_usage(converted))

    return converted