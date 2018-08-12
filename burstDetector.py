def render(table, params):
    date_column_name = params['date_column_name']
    interval_length = params['interval_length']
    interval_unit = params['interval_unit']
    trigger_threshold = params['trigger_threshold']

    if not date_column_name:
        return table

    timedelta_units = ['seconds', 'minutes', 'hours', 'days', 'weeks']
    timedelta_unit = timedelta_units[interval_unit]
    timedelta = pd.Timedelta(**{timedelta_unit: interval_length})

    timestamps = pd.to_datetime(table[date_column_name]).dropna().sort_values().reset_index(drop=True)
    bursts_start = timestamps.shift(trigger_threshold - 1)

    is_end_of_burst = pd.concat(((timestamps - bursts_start) < timedelta, pd.Series([False] * (trigger_threshold - 1))))

    last_burst_index = (is_end_of_burst * np.arange(len(is_end_of_burst))).replace(0, np.nan).fillna(method='pad')

    since_last_burst = pd.Series(np.arange(len(last_burst_index))) - last_burst_index.reset_index(drop=True)
    in_burst = since_last_burst.shift(-(trigger_threshold - 1))[:len(timestamps)] < trigger_threshold

    # https://stackoverflow.com/questions/40802800
    bursts = pd.DataFrame({
        'in_burst': in_burst,
        'timestamp': timestamps,
    })
    bursts['new_burst'] = bursts['in_burst'] & (bursts['in_burst'].shift() != True)
    bursts = bursts[bursts['in_burst']]
    bursts['burst_id'] = bursts['new_burst'].cumsum()
    merged_bursts = (bursts
        .groupby('burst_id')
        .apply(lambda df: pd.Series([
            df['timestamp'].iloc[0],
            df['timestamp'].iloc[-1],
            df.shape[0] - 1])))
    merged_bursts.columns = ['start', 'end', 'num_events']
    return merged_bursts
