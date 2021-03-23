from configparser import ConfigParser 
import os


class EnvConfigParser:
    def __init__(self):
        self.parser = ConfigParser()

    def get_parser(self, configfile, defaults):
        self.parser.read(configfile)

        for section in self.parser.sections():
            for var in self.parser.items(section):
                var_name = var[0]
                env_var_name = "{}_{}".format(section.upper(), var_name.upper())
                env_var_val = os.getenv(env_var_name)
                if env_var_val:
                    self.parser.set(section, var_name, env_var_val)
                
                if not self.parser.get(section, var_name) and env_var_name in defaults.keys():
                    self.parser.set(section, var_name, defaults[env_var_name])
        
        return self.parser

        
