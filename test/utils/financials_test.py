import pytest
from src.utils.financials import calc_earnings, calc_losses, calc_apr

import numpy as np

EPOCHS_PER_YEAR = 82179.45
GWEI = 10 ** 9
PRINCIPAL = 24 * 10 ** 18

def get_random_array(size):
    return (np.random.rand(size) * 33 * 10 ** 9).astype(int).tolist()


def test_calc_earnings():
    assert calc_earnings([4, 3, 2, 1]) == 3
    assert calc_earnings([1, 1, 1, 1]) == 0
    assert calc_earnings([10, 0, 1, 30]) == 10
    assert calc_earnings([]) == 0
    assert calc_earnings([1000]) == 0


    random_arr = get_random_array(int(np.random.rand(1) * 100000))
    result = 0

    for index in range(len(random_arr) - 1):
        current_element = random_arr[index]
        next_element = random_arr[index + 1]
        result += current_element - next_element if current_element > next_element else 0

    assert result == calc_earnings(random_arr)


def test_calc_losses():
    assert calc_losses([7, 8, 9, 10]) == 3
    assert calc_losses([10, 9, 8, 7]) == 0
    assert calc_losses([]) == 0
    assert calc_losses([10000]) == 0
    assert calc_losses([90, 20, 50, 0]) == 30


    random_arr = get_random_array(int(np.random.rand(1) * 100000))
    result = 0

    for index in range(len(random_arr) - 1):
        current_element = random_arr[index]
        next_element = random_arr[index + 1]
        result += next_element - current_element if next_element > current_element else 0

    assert result == calc_losses(random_arr)


def test_calc_apr():
    assert calc_apr(0, 1000) == 0

    max_earnings = 0.3 * GWEI
    min_epochs = 1

    earnings = int(np.random.rand(1) * max_earnings)
    earnings_wei = earnings * GWEI
    number_of_epochs = int(np.random.rand(1) * 1000) + min_epochs
    yearly_earnings = earnings_wei / number_of_epochs * EPOCHS_PER_YEAR
    apr = yearly_earnings / PRINCIPAL * 100

    assert apr == calc_apr(earnings, number_of_epochs)
