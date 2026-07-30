FROM custompodautoscaler/python:latest
ADD requirements.txt /
RUN pip install -r /requirements.txt
ADD config.yaml evaluate.py metric.py run_plugin.py /
ADD plugins /plugins
