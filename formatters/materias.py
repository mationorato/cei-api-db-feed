import pandas as pd

cols=[
    'codigo','nombre','curso',
    'profesores','dia','hora_inicio',
    'hora_fin','aula','sede'
]

types = { i:str for i in cols }

def load(path):
    df = pd.read_csv(path, usecols = cols, dtype=types)

    # dataframe format
    df["nombre"] = df["nombre"].apply(lambda x: x.title())
    df["nombre"] = df["nombre"].str.replace("Ii","II")
    df["nombre"] = df["nombre"].str.replace("IIi","III")
    df["nombre"] = df["nombre"].str.replace("Vi","VI")
    df["profesores"] = df["profesores"].apply(lambda x: x.title())
    df["codigo"] = df["codigo"].apply(lambda x: x[:2] + '.' + x[2:])
    
    # list of dicts creation
    materias = []
    gdf_cod = df.groupby(['codigo','nombre'])
    for name_cod,df_cod in gdf_cod:
        df_cod = df_cod.drop(['codigo', 'nombre'], axis=1)
        materia = {
            'codigo': name_cod[0],
            'nombre': name_cod[1],
            'cursos_vigentes': []
        }
        gdf_cur = df_cod.groupby(['curso','profesores'])
        for name_cur,df_cur in gdf_cur:
            df_cur = df_cur.drop(['curso', 'profesores'], axis=1)
            curso = {
                'curso_id': None,
                'numero': name_cur[0],
                'docentes': name_cur[1].split(' - '),
                'horarios': df_cur.to_dict('records')
            }
            materia['cursos_vigentes'].append(curso)
        materias.append(materia)
        
    return materias





