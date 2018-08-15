def detect_bursts(timestamps, window, threshold):
    timestamps = timestamps.dropna().sort_values().reset_index(drop=True)
    bursts_start = timestamps.shift(threshold - 1)

    is_end_of_burst = pd.concat(((timestamps - bursts_start) < window,
                                 pd.Series([False] * (threshold - 1))))

    last_burst_index = ((is_end_of_burst * np.arange(len(is_end_of_burst)))
                        .replace(0, np.nan)
                        .fillna(method='pad'))

    since_last_burst = (pd.Series(np.arange(len(last_burst_index)))
                        - last_burst_index.reset_index(drop=True))
    in_burst = (since_last_burst.shift(-(threshold - 1))[:len(timestamps)]
                < threshold)

    # https://stackoverflow.com/questions/40802800
    new_burst = in_burst & (in_burst.shift() != True)
    bursts = pd.DataFrame({
        'new_burst': new_burst,
        'timestamp': timestamps,
    })
    bursts = bursts[in_burst]
    bursts['burst_id'] = bursts['new_burst'].cumsum()
    merged_bursts = (bursts.groupby('burst_id')
                     .apply(lambda df: pd.Series([
                         df['timestamp'].iloc[0],
                         df['timestamp'].iloc[-1],
                         df['timestamp'].iloc[-1] - df['timestamp'].iloc[0],
                         df.shape[0] - 1])))

    merged_bursts_columns = ['start', 'end', 'duration', 'num_events']
    if not merged_bursts.empty:
        merged_bursts.columns = merged_bursts_columns
        return merged_bursts
    else:
        return pd.DataFrame(columns=merged_bursts_columns)


def render(table, params):
    date_column_name = params['date_column_name']
    interval_length = params['interval_length']
    interval_unit = params['interval_unit']
    threshold = params['trigger_threshold']

    if not date_column_name:
        return table

    timedelta_units = ['seconds', 'minutes', 'hours', 'days', 'weeks']
    timedelta_unit = timedelta_units[interval_unit]
    window = pd.Timedelta(**{timedelta_unit: interval_length})

    return detect_bursts(pd.to_datetime(table[date_column_name]),
                         window,
                         threshold)
