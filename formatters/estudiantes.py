import pandas as pd

cols=[
    'documento','padron','apellido','nombre'
]

types = { i:v for (i,v) in zip(cols,[str,int,str,str]) }


def load(path):
    
    df = pd.read_csv(path, usecols= cols, dtype=types)

    # dataframe fotmat    
    df["cursadas_vigentes"] = None
    df["nombre"] = df["nombre"].apply(lambda x: x.title())
    df["apellido"] = df["apellido"].apply(lambda x: x.title())
    
    # list of dicts creation
    estudiantes = df.to_dict('records')

    return estudiantes

