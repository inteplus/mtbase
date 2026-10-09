"""Common tools for jupyter.

Contains :func:`display_page_in_jupyter`, which embeds a web page in a notebook cell. It needs
IPython installed.
"""


def display_page_in_jupyter(url, width=980, height=800):
    from IPython.display import IFrame

    return IFrame(url, width=width, height=height)
