from itertools import islice
from multiprocessing import cpu_count, Process, Pool
from time import time
import logging

logger = logging.getLogger()
stream_handler = logging.StreamHandler()
logger.addHandler(stream_handler)
logger.setLevel(logging.DEBUG)


def chunk(l, n):
    for i in range(0, len(l), n):
        yield l[i:i + n]


def factorize_sync(numbers: list) -> list[int]:
    """
    Method that filters the list of input numbers and returns the even numbers.
    :param numbers: Input list of numbers.
    :return: Even numbers.
    :param numbers:
    :return:
    """
    return list(filter(lambda n: n % 2 == 0, numbers))


def factorize_async(numbers: list) -> list[int]:
    """
    Method that filters the list of input numbers and returns the even numbers. This job
    is divided among all cores in the system.
    :param numbers: Input list of numbers.
    :return: Even numbers.
    """
    cores = cpu_count()
    multiplier = 1000
    nums_chunks = list(chunk(numbers, cores * multiplier))
    result_numbers = []
    with Pool(processes=cores) as pool:
        lists = pool.map(factorize_sync, nums_chunks)
    for _list in lists:
        result_numbers += _list
    return result_numbers


if __name__ == "__main__":
    nums = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10] * 1000_000_0  # 1000_000_0
    #################### Sync run ######################
    start_time = time()
    logger.debug(factorize_sync.__name__)
    result = factorize_sync(numbers=nums)
    end_time = time()
    duration = end_time - start_time
    logger.debug(f"{duration = }")
    ####################################################

    #################### Multiprocess run ######################
    start_time = time()
    logger.debug(factorize_async.__name__)
    factorize_async(numbers=nums)
    end_time = time()
    duration = end_time - start_time
    logger.debug(f"{duration = }")

    # 5.867555141448975 - with multiprocessing
    # 8.04298996925354 - wout multiprocessing
