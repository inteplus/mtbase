"""Traceback extra"""

import traceback as _tb
from traceback import *


__all__ = ['format_exc_info', 'extract_stack_compact']


def format_exc_info(exc_type, exc_value, exc_traceback):
    '''Formats (exception type, exception value, traceback) into multiple lines.'''
    statements = _tb.format_exception(exc_type, exc_value, exc_traceback)
    statements = "".join(statements)
    return statements.split('\n')

def extract_stack_compact():
    '''Returns the current callstack in a compact format.'''
    lines = _tb.format_list(_tb.extract_stack())
    lines = "".join(lines).split('\n')
    lines = [line for line in lines if 'frozen importlib' not in line]
    return lines
