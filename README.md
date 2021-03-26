# aiven-workhome



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
1. Customize config files, particulary kafka and postgres config
1. Note there is a common folder containing a bunch of files. These are the common modules for both, producer and consumer. It is needed to, after creating the virtualenv, add it to the PYTHONPATH by running `add2virtualenv common`.

## What to improve
1. feature to define mandatory config vars and type/casting
1. Accept postgresql connection data separately and not only in a uri
1. Do some kind of config check (is the db name included in the uri?)
1. Kafka consumer commit. Enable/Disable. Probably there are stuations where it is interesting to not commit the consumer and read every time from the beginning of the queue.





TABLE WITH THE DIFFERENT NAMES OF FIELDS IN DIFFERENT PARTS, CONFIG FILE, ENV VARS, JSON AND DATABASE