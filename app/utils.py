import time

def now_ms():
    '''
    Return the current time before server API call
    '''
    return str(int(time.time() * 1000))
