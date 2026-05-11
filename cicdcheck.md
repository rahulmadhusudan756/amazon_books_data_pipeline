Run docker compose -f docker-compose.yaml exec -T airflow-webserver airflow dags list
time="2026-05-11T04:44:09Z" level=warning msg="The \"AIRFLOW_UID\" variable is not set. Defaulting to a blank string."
time="2026-05-11T04:44:09Z" level=warning msg="The \"AIRFLOW_UID\" variable is not set. Defaulting to a blank string."
Unable to load the config, contains a configuration error.
Traceback (most recent call last):
  File "/usr/local/lib/python3.12/pathlib.py", line 1311, in mkdir
    os.mkdir(self, mode)
FileNotFoundError: [Errno 2] No such file or directory: '/opt/airflow/logs/scheduler/2026-05-11'

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/usr/local/lib/python3.12/logging/config.py", line 581, in configure
    handler = self.configure_handler(handlers[name])
              ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/usr/local/lib/python3.12/logging/config.py", line 854, in configure_handler
    result = factory(**kwargs)
             ^^^^^^^^^^^^^^^^^
  File "/home/airflow/.local/lib/python3.12/site-packages/airflow/utils/log/file_processor_handler.py", line 53, in __init__
    Path(self._get_log_directory()).mkdir(parents=True, exist_ok=True)
  File "/usr/local/lib/python3.12/pathlib.py", line 1315, in mkdir
    self.parent.mkdir(parents=True, exist_ok=True)
  File "/usr/local/lib/python3.12/pathlib.py", line 1311, in mkdir
    os.mkdir(self, mode)
PermissionError: [Errno 13] Permission denied: '/opt/airflow/logs/scheduler'

The above exception was the direct cause of the following exception:

Traceback (most recent call last):
  File "/home/airflow/.local/bin/airflow", line 5, in <module>
    from airflow.__main__ import main
  File "/home/airflow/.local/lib/python3.12/site-packages/airflow/__init__.py", line 74, in <module>
    settings.initialize()
  File "/home/airflow/.local/lib/python3.12/site-packages/airflow/settings.py", line 531, in initialize
    LOGGING_CLASS_PATH = configure_logging()
                         ^^^^^^^^^^^^^^^^^^^
  File "/home/airflow/.local/lib/python3.12/site-packages/airflow/logging_config.py", line 74, in configure_logging
    raise e
  File "/home/airflow/.local/lib/python3.12/site-packages/airflow/logging_config.py", line 69, in configure_logging
    dictConfig(logging_config)
  File "/usr/local/lib/python3.12/logging/config.py", line 920, in dictConfig
    dictConfigClass(config).configure()
  File "/usr/local/lib/python3.12/logging/config.py", line 588, in configure
    raise ValueError('Unable to configure handler '
ValueError: Unable to configure handler 'processor'
Error: Process completed with exit code 1.