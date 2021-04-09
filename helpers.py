import pandas as pd
import unidecode

def dtf(x):
    return x.strftime("%d/%m/%Y")


def clean_names(s: pd.Series, to_remove=[]):
    s2 = s.map(lambda x: unidecode.unidecode(x))
    for c in to_remove:
        s2 = s2.str.replace(c, " ", regex=False)
    return s2.str.strip().str.split().str.join("_").str.lower()
