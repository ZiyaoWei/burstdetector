def render(table, params):
    date_column_name = params['date_column_name']
    interval_length = params['interval_length']
    interval_unit = params['interval_unit']
    trigger_threshold = params['trigger_threshold']

    if not date_column_name:
        return table

    import pandas as pd
    import pandas.tseries.offsets as offsets

    offset_units = [
        offsets.Second,  # sec
        offsets.Minute,  # min
        offsets.Hour,    # hour
        offsets.Day,     # day
        offsets.Week     # week
    ]
    offset_unit = offset_units[interval_unit]
    offset = offset_unit(interval_length)

    # Preprocess the dataframe for `rolling`:
    # 1) sort the index to monotonically increasing
    # 2) Add a column of 1s so rolling works
    timestamps = pd.DataFrame(index=table[date_column_name]).sort_index()
    timestamps['count'] = [1] * timestamps.shape[0]

    bursts = timestamps.rolling(offset, min_periods=trigger_threshold).count()
    bursts['is_burst'] = bursts['count'].isnull()

    # https://stackoverflow.com/questions/40802800
    bursts['burst_start'] = bursts['is_burst'] != bursts['is_burst'].shift()
    bursts = bursts.dropna()
    bursts['burst_id'] = bursts['burst_start'].cumsum()
    bursts = (bursts
              .groupby('burst_id')
              .apply(lambda df: pd.Series([
                  df.index[0],
                  df.index[-1],
                  df['count'][0] + df.shape[0] - 1])))
    bursts.columns = ['start', 'end', 'num_events']
    return bursts
