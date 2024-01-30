from statistics import mean
from datetime import datetime, timedelta
from itertools import groupby
from operator import itemgetter

def form_index_record(data_point):
    first_data_entry = data_point[0]
    second_data_entry = data_point[1]
    
    earnings = float(mean([entry['earnings'] for entry in second_data_entry]))
    losses = float(mean([entry['losses'] for entry in second_data_entry]))
    apr = float(mean([entry['apr'] for entry in second_data_entry]))

    return {
        'earnings': earnings,
        'losses': losses,
        'apr': apr,
        'indexId': str(first_data_entry)
    }

def top_indexes(data_point):
    first_data_entry = data_point[0]
    second_data_entry = data_point[1]
    
    earningsPerValidator = float(mean([entry['earnings'] for entry in second_data_entry]))
    lossesPerValidator = float(mean([entry['losses'] for entry in second_data_entry]))
    aprPerValidator = float(mean([entry['apr'] for entry in second_data_entry]))
    earningsAbsolute = float(sum([entry['earnings'] for entry in second_data_entry]))
    lossesAbsolute = float(sum([entry['losses'] for entry in second_data_entry]))

    return {
        'earningsPerValidator': earningsPerValidator,
        'lossesPerValidator': lossesPerValidator,
        'aprPerValidator': aprPerValidator,
        'earningsAbsolute': earningsAbsolute,
        'lossesAbsolute': lossesAbsolute,
        'indexId': str(first_data_entry)
    }

def index_apr(data_point):
    first_data_entry = data_point[0]
    second_data_entry = data_point[1]
    
    aprPerValidator = float(mean([entry['apr'] for entry in second_data_entry]))

    return {

        'aprPerValidator': aprPerValidator,
        'indexId': str(first_data_entry)
    }

def get_time_with_lag(lag=0):
 return datetime.strftime(datetime.now() - timedelta(lag), '%Y-%m-%d')

def get_validators_with_indices(validators):
    return [(index, list(values)) for index, values in groupby(validators, key= lambda x: x["savETHIndex"])]