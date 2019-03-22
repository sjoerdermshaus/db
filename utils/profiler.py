import cProfile
import pyprof2calltree
from functools import wraps
from datetime import datetime


def profiler(file=None):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            prof = None

            # -- BEFORE WRAP --
            if file is not None:
                prof = cProfile.Profile()
                prof.enable()
            # -- BEFORE WRAP --

            f = func(*args, **kwargs)

            # -- AFTER WRAP --
            if file is not None:
                prof.disable()
                prof.dump_stats(file)
                stats = prof.getstats()
                # pyprof2calltree.visualize(stats)
                pyprof2calltree.convert(stats, f'qcachegrind_{file}')
            # -- AFTER WRAP --

            return f
        return wrapper
    return decorator


def runtime():
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):

            # -- BEFORE WRAP --
            print('-' * 31)
            start_time = datetime.now()
            print('Start time:', str(start_time).split('.')[0])
            print('-' * 31)
            # -- BEFORE WRAP --

            f = func(*args, **kwargs)

            # -- AFTER WRAP --
            end_time = datetime.now()
            print('-' * 31)
            print('End time:', str(end_time).split('.')[0])
            td = end_time - start_time  # timedelta object
            minutes, seconds = divmod(td.seconds, 60)
            hours, minutes = divmod(minutes, 60)
            hours = td.days * 24 + hours
            print('Runtime (h:mm:ss):', f'{hours}:{minutes:02d}:{seconds:02d}')
            print('-' * 31)
            # -- AFTER WRAP --

            return f
        return wrapper
    return decorator


def main():
    start_time = datetime.strptime('2018-07-12 10:36:16', '%Y-%m-%d %H:%M:%S')
    print('Start time:', str(start_time).split('.')[0])
    end_time = datetime.now()
    print('End time:', str(end_time).split('.')[0])
    print('Runtime:', str(end_time - start_time).split('.')[0])
    td = end_time - start_time
    minutes, seconds = divmod(td.seconds, 60)
    hours, minutes = divmod(minutes, 60)
    hours = td.days * 24 + hours
    print('Runtime (h:mm:ss):', f'{hours}:{minutes:02d}:{seconds:02d}')


if __name__ == '__main__':
    main()
