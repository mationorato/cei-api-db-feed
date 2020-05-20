import os

def set_enviroment_vars(dic):
    for key,value in dic.items():
        if type(value) is dict:
            set_enviroment_vars(value)
        elif type(value) is str:
            if "env:" in value:
                env_var = value.replace("env:","")
                dic[key] = os.environ[env_var]  