import os
import json
from pathlib import Path
from enviroment import set_enviroment_vars
import pandas as pd
import pymongo as pm
import formatters.estudiantes as formatter_est
import formatters.materias as formatter_mat

class IndexType:
    SINGLE = "single"
    SINGLE_UNIQUE = "single_unique"
    TEXT = "text"

def submit_collect(nombre,mongo_collect,loader,path,indexes):
    print("■ %s" % nombre.upper())
    print("> Cargando datos...")
    try:
        collection = loader(path)
    except:
        print("- ERROR: formato invalido")    
        return

    print("> Creando Indices...")
    for index in indexes:
        if index['type'] == IndexType.SINGLE:
            mongo_collect.create_index(
                index['index'], name=index['name'], unique=False)
        elif index['type'] == IndexType.SINGLE_UNIQUE:
            mongo_collect.create_index(
                index['index'], name=index['name'], unique=True)
        elif index['type'] == IndexType.TEXT:
            mongo_collect.create_index(
                index['index'], name=index['name'], default_language=index['language'])

    print("> Grabando Colección...")
    creados = 0
    duplicados =0
    for doc,index in zip(collection, [i for i in range(len(collection))]):
        try:
            mongo_collect.insert_one(doc)            
            creados += 1            
        except pm.errors.DuplicateKeyError:
            duplicados += 1
        finally:        
            print("· Progreso: %.1f %%" % (100*(index+1)/len(collection)), end="\r")

    print("+ Operación Finalizada")
    print("-- creados: %i de %i" % (creados, len(collection)))
    print("-- duplicados: %i de %i - (no creados)" % (duplicados, len(collection)))
    print("-- totales: %i de %i" % (creados + duplicados, len(collection)))


def feed_db(settings):
    print("- CEI API DATA FEED -")

    # Conexión Con Mongo DB
    print("> Conectando con MongoDB")
    try:
        client = pm.MongoClient(settings["server"]["connection_string"])
        database = client[settings["database"]["name"]]
        collec_estudiantes = database[settings["database"]["collections"]["estudiantes"]]
        collect_materias = database[settings["database"]["collections"]["materias"]]
    except pm.errors.PyMongoError as e:
        print("- ERROR: Se detuvo la carga, revisar conexión con Mongo DB")
        print("-- Mensaje: ", e)        
        return

    #Alta Estudiantes
    submit_collect(
        nombre = 'Estudiantes',
        mongo_collect = collec_estudiantes,
        loader = formatter_est.load,
        path = settings['data']['estudiantes'],
        indexes = [
            {
                'type': IndexType.SINGLE_UNIQUE,
                'index':[('padron',pm.DESCENDING)],
                'name':"padron_desc"
            },
            {
                'type': IndexType.SINGLE_UNIQUE,
                'index':[('documento',pm.DESCENDING)],
                'name':"documento_desc",                
            },
            {
                'type': IndexType.TEXT,
                'index':[('nombre',pm.TEXT),('apellido',pm.TEXT)],
                'name':"nombre_apellido_text",
                'language':"spanish"
            }
        ]
    )

    # Alta Materias
    submit_collect(
        nombre = 'Materias',
        mongo_collect = collect_materias,
        loader = formatter_mat.load,
        path = settings['data']['cursos'],
        indexes = [
            {
                'type': IndexType.SINGLE_UNIQUE,
                'index':[('codigo',pm.DESCENDING)],
                'name':"codigo_desc"
            },
            {
                'type': IndexType.TEXT,
                'index':[('nombre',pm.TEXT),('cursos_vigentes.docentes',pm.TEXT),('codigo',pm.TEXT)],
                'name':"codigo_nombre_docentes_text",
                'language':"spanish"
            }
        ]
    )         

    print("+ Carga Finalizada")


         


def load_settings(filename):
    with open(Path(filename), "r") as settings_file:
        settings = json.load(settings_file)
    
    set_enviroment_vars(settings)

    return settings

if __name__ == "__main__":
    settings = load_settings("settings.json")
    feed_db(settings)

