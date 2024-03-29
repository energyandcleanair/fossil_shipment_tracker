import pandas as pd
import numpy as np
from colorir import *

palette_map = {
    "China": "#990000",
    "EU": "#8cc9D0",
    "India": "#f6b26b",
    "United States": "#35416C",
    "Turkey": "#27a59c",
    "For orders": "#FFF2CC",
    "Others": "#cacaca",
    "Other": "#cacaca",
    "United Kingdom": "#741b47",
    "Unknown": "#333333",
    "Russia": "#660000",
    "United Arab Emirates": "#741b47",
    "South Korea": "#351c75",
    "Coal": "#351c75",
    "LNG": "#f6b26b",
    "Pipeline gas": "#f6b26b80",
    "Gas": "#f6b26b",
    "Crude oil": "#741b47",
    "Oil": "#741b47",
    "Oil products and chemicals": "#741b4760",
    "Owned and / or insured in EU & G7": "#8cc9D0",
    "Insured in Norway": "#35416C",
}

palette = {
    "dark_blue": "#35416C",
    "blue": "#8cc9D0",
    "light_blue": "#cce7eb",
    "yellow": "#fff2cc",
    "orange": "#f6b26b",
    "red": "#cc0000",
    "dark_red": "#990000",
    "dark_purple": "#741b47",
    "dark_violet": "#351c75",
    "light_gray": "#cacaca",
    "turquoise": "#27a59c",
    "green": "#75b44c",
}

palette_grad = Grad(Palette(palette))


def get_palette(n):
    if n <= len(palette.values()):
        return list(palette.values())[:n]
    else:
        return palette_grad.n_colors(n)


def to_list(d, convert_tuple=False):
    if d is None:
        return []
    if convert_tuple and isinstance(d, tuple):
        return list(d)
    if not isinstance(d, list):
        return [d]
    else:
        return d


def opaque_background(fig):
    for i in range(len(fig["data"])):
        fig["data"][i]["line"]["width"] = 1
        fig["data"][i]["fillcolor"] = fig["data"][i]["line"].color

    return fig
