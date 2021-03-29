# aiven-workhome
This is the documentation for the homework included in the Aiven selection process

A brief explanation of the task could be:
```
Create a few python scripts to check a web-site availability and some other parameters and send the info, through kafka producer-consumer, to a postgresql db.

The Kafka service as well as the PostgreSQL service should be offered by aiven
```

# Features
1. High volume management:
    1. Used threads to check the site and inserts into the db
    1. Inserts multiple queries at the same time if needed. Max number of inserts can be configured
1. Config parameters can be defined in the config file and as env vars. Env vars takes precedende over config files.
1. It is supossed this check should be used to identify and determine a site availability problem. An http get includes, as many times, a DNS resolv before. Both parts are done independently in the test and stored in the message.
1. The checks are run very regulary. The checks are executed in a separated thread to prevent a wrong delay time between checks. If the check it is run in the same thread, the delay time will be added to the check time until next iteration starts, and that is not a best practice.
1. CTRL-C captured and managed



# Project layout
The project layout is very common:
1. `src` dir: Project code itself. Different modules in different dirs.
1. `tests` dir: Tests, different modules tests in different dirs.

## src/common
It was expected to find more overlapping code at the very beginning of the project, but finally it was only the way the config is managed
### envconfigparser
It is a good idea to be able to configure the processes in a container by different methods. I personally find useful env vars and config files, so you can use configmaps in kubernetes. Long commands in the format of a kubernetes container command is not something very readable to me.
If a var is defined in both places, the env var takes precedende.

To get this idea working I wrote a sime class that:
1. load the config from file as a regular ConfigParser object
1. Goes section by section and var by var trying to find that `SECTION_VAR` (note the uppercase) as an env var and ,if it exist, the var is updated with the env var value.
1. Returns the updated `ConfigParser` object

## src/producer
The producer part. Check a url defined in the config and send the info to kafka
### src/producer/producer.py
Main producer file

### src/producer/httpchecker.py
Class that abstacts the HTTP Check. It receives an URL, timeout and optionally a regex as parameter, runs an HTTP Get and returns a json with the info collected.

## src/consumer
The consumer part. Reads messages from a kafka queue and inserts them into a postgresql db.

### src/consumer/consumer.py
Main producer file

### src/consumer/checksdb.py
Class that abstacts the db management from the main module.




# Run it!
## Prerequisites
1. Python 3.6 or above
1. kafka running and auto-create topic enabled
1. PostgerSQL running
1. Customize config files, particulary kafka and postgres config
1. Create two virtual envs, one for the consumer and another for the producer, and install the packages defined in each requisites.txt file in each folder.
1. Add the root of the project to both of the virtual envs by running: `add2virtualenv $PROJECT_ROOT_PATH`.

## Go!
It is needed, as requested in the project description, to run both consumer and producer separately. The steps are the same for both:
1. Activate virtualenv
1. Run it by:
    1. python src/consumer/consumer.py
    1. python src/producer/producer.py

## Stop!
Stop the execution by sending a `SIGINT` (`kill -2 $PID`) to the process or pressing `CTRL-C`. Connections are managed properly.



# Data
Table that shows the name of the fields in the json and the DB, as well as the meaning of each field.

# Kafka msg json
```json
{
   "meta":{
      "host":"example.net"
   },
   "dns":{
      "start":1616980630.7671506,
      "elapsed":208118,
      "ip":"93.184.216.34"
   },
   "http":{
      "start":1616980630.9762378,
      "schema":"https",
      "host":"example.net",
      "path":null,
      "url":"https://example.net",
      "regex":"\.\s+You\s+may\s+use",
      "status_code":200,
      "elapsed":520368,
      "reason":"OK",
      "regex_found":false
   }
}
```

# DB Scheme
A single table is created in the DB, this are the fields:
1. `id` SERIAL PRIMARY KEY,
1. `host` VARCHAR(256),
1. `dns_start` FLOAT,
1. `dns_elapsed` INT,
1. `ip` VARCHAR(15),
1. `http_start` FLOAT,
1. `http_elapsed` INT,
1. `http_schema` VARCHAR(10),
1. `http_url_root` VARCHAR(256),
1. `http_path` VARCHAR(256),
1. `http_url` VARCHAR(512),
1. `http_regex` VARCHAR(512),
1. `http_status_code` INT,
1. `http_status_code_reason` VARCHAR(512),
1. `http_retgex_found` BOOLEAN

## Fields mapping and meaning
| DB                      | json             | Meaning                                                                          |
|-------------------------|------------------|----------------------------------------------------------------------------------|
| Id                      | -                | Id field in db and PK                                                            |
| host                    | meta.host        | host to resolv                                                                   |
| dns_start               | dns.start        | when dns rolution started                                                        |
| dns_elapsed             | dns.elapsed      | how long takes the dns resolution                                                |
| ip                      | dns.ip           | ip of the host. Will replace the http.host string in the http.url.               |
| http_start              | http.start       | when http get started                                                            |
| http_elapsed            | http.elapsed     | how long takes the http get request                                              |
| http_schema             | http.schema      | http or https                                                                    |
| http_url_root           | http.host        | host to use in the header of the http request, as it is not included in the url. |
| http_path               | http.path        | Path part of the query                                                           |
| http_url                | http.url         | original url defined in the config                                               |
| http_regex              | http.regex       | Regex to apply to the site tested                                                |
| http_status_code        | http.status_code | Status code of the request                                                       |
| http_status_code_reason | http.reason      | Description of the status code                                                   |
| http_regex_found        | http.regex_found | True or false depending if the http.regex matches                                |
|                         |                  |                                                                                  |
|                         |                  |                                                                                  |
|                         |                  |                                                                                  |


## What to improve
1. feature to define mandatory config vars and type/casting
1. Accept postgresql connection data separately and not only in a uri
1. Do some kind of config checks
1. Feature to enable/disable Kafka consumer commits. Probably there are stuations where it is interesting to not commit the consumer and read every time from the beginning of the queue.
1. Receive config file path as parameter or env var
1. Didn't know of the json adapter included in psycopg2, so it could be used instead of writing it manually.
1. Try to reconnect if connection to db is lost
1. execute function to avoid cursor create, execute, close
1. Remove possible duplicated log events
1. If SIGINT received when the flow is outside of the main loop, the exit is not controlled
1. Config file explained in doc, not only inline.
1. Classes explained in the doc, not only in the code.


