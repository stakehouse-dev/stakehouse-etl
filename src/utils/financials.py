from src.utils.constants import EPOCHS_PER_YEAR, ONE_GWEI, DETH_PRINCIPAL


def calc_apr(earnings, number_of_epochs):
    earnings_in_wei = earnings * ONE_GWEI
    earnings_per_epoch = earnings_in_wei / number_of_epochs
    extrapolated_annual_earnings = earnings_per_epoch * EPOCHS_PER_YEAR

    return extrapolated_annual_earnings / DETH_PRINCIPAL * 100

def compute_deltas(data):
    if len(data) < 2:
        raise Exception('Need at least 2 data points to compute differences')

    # Here entries are assumed to be in chronological descending order
    return [data[i] - data[i + 1] for i in range(len(data) - 1)]


def calc_earnings(data):
    results_length = len(data)

    if results_length <= 1:
        return 0

    return sum([d for d in compute_deltas(data) if d > 0])


def calc_losses(data):
    results_length = len(data)

    if results_length <= 1:
        return 0

    return -1 * sum([d for d in compute_deltas(data) if d < 0])
