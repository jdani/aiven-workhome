# aiven-workhome

Note there is a common folder containing a bunch of files. These are the common modules for both, producer and consumer. It is needed to, after creating the virtualenv, add it to the PYTHONPATH by running `add2virtualenv common`.

## What is there in common?
### envconfigparser
It is a good idea to be able to configure the processes in a container by different methods. I personally find useful env vars and config files, so you can use configmaps in kubernetes. Long commands in the format of a kubernetes container command is not something very readable to me.
If a var is defined in both places, the env var takes precedende.

To get this idea working I wrote a sime class that:
1. load the config from file as a regular ConfigParser object
1. Goes section by section and var by var trying to find that `SECTION_VAR` (note the uppercase) as an env var and ,if it exist, the var is updated with the env var value.
1. Returns the updated `ConfigParser` object



## Prerequisites
1. kafka running and auto-create topic enabled

## What to improve
1. feature to define mandatory config vars and type
