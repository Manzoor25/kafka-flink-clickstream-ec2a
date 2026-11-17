FROM grafana/grafana:13.3.1
RUN grafana cli plugins install grafana-clickhouse-datasource 4.23.5
COPY provisioning/ /etc/grafana/provisioning/
COPY dashboards/ /var/lib/grafana/dashboards/
