from configparser import ConfigParser 
import os


class NoDefaultValue(Exception):
    pass


class EnvConfigParser:
    def __init__(self):
        # Initialize ConfigParser instance
        self.parser = ConfigParser()

    def get_parser(self, configfile, defaults):
        # Read the config file only when needed
        self.parser.read(configfile)

        # For each section...
        for section in self.parser.sections():
            # for each var...
            for var in self.parser.items(section):
                var_name = var[0]
                # Build the env var name: SECTION_VAR_NAME
                env_var_name = "{}_{}".format(section.upper(), var_name.upper())
                # Get the env var
                env_var_val = os.getenv(env_var_name)
                # If there is such a env var...
                if env_var_val:
                    # ...override the value in the config file
                    self.parser.set(section, var_name, env_var_val)
                
                # If the var is not defined in the config file or as env var, assign default
                if not self.parser.get(section, var_name) and env_var_name in defaults.keys():
                    self.parser.set(section, var_name, defaults[env_var_name])

                # If the var is still not set
                if not self.parser.get(section, var_name):
                    raise NoDefaultValue('No default value set for var "{}"'.format(env_var_name))
        
        return self.parser

        
