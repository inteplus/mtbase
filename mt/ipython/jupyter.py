"""Common tools for jupyter."""


def display_page_in_jupyter(url, width=980, height=800):
    from IPython.display import IFrame

    return IFrame(url, width=width, height=height)
