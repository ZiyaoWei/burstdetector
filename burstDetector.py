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

    timestamps = pd.to_datetime(table[date_column_name]).dropna().sort_values()
    bursts = pd.DataFrame({
        'start': timestamps.shift(trigger_threshold - 1),
        'end': timestamps,
    })
    bursts['is_burst'] = (bursts['end'] - bursts['start']) < timedelta

    # https://stackoverflow.com/questions/40802800
    bursts = bursts[:100]
    bursts['new_burst'] = bursts['is_burst'] & (bursts['is_burst'].shift() != True)
    print(bursts.to_string())
    bursts = bursts[bursts['is_burst']]
    bursts['burst_id'] = bursts['new_burst'].cumsum()
    bursts = bursts[:100]
    # print(bursts)
    merged_bursts = (bursts
        .groupby('burst_id')
        .apply(lambda df: pd.Series([
            df['start'].iloc[0],
            df['end'].iloc[-1],
            trigger_threshold + df.shape[0] - 1])))
    merged_bursts.columns = ['start', 'end', 'num_events']
    return merged_bursts
