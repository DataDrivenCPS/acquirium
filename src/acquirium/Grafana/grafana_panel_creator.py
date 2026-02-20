from grafanalib.core import (
    SqlTarget,GridPos, GaugePanel, Threshold, BarChart, StateTimeline, TimeSeries
)

'''
prop_dict=
{
  "point_uri": "urn:ex/point1",
  "ref_uri": "http://example.com/point1",
}

'''



def create_gauge(prop_dict,height=3,width=2,x=0,y=0):
    if "#" in prop_dict["point_uri"]:
        point_name = str(prop_dict["point_uri"]).split("#")[-1]
    elif "/" in prop_dict["point_uri"]:
        point_name = str(prop_dict["point_uri"]).split("/")[-1]
    else:
        point_name = str(prop_dict["point_uri"])
    gauge = GaugePanel(
        title=point_name,
        dataSource="grafana-postgresql-datasource-1",
        targets=[
            SqlTarget(rawSql=f'''
                SELECT CAST(value as DECIMAL) 
                FROM timeseries 
                WHERE point_uri='{str(prop_dict["ref_uri"]).strip("<>")}' ORDER BY ts DESC LIMIT 1;
                      ''', format="table")
            ],
        gridPos=GridPos(h=height, w=width, x=x, y=y),
    )
    return gauge


def create_time_series(prop_dicts:list[dict],title:str,height=4,width=12,x=0,y=0):
    point_names = []
    for point in prop_dicts:
        if "#" in point["point_uri"]:
            point_names.append(str(point["point_uri"]).split("#")[-1])
        elif "/" in point["point_uri"]:
            point_names.append(str(point["point_uri"]).split("/")[-1])
        else:
            point_names.append(str(point["point_uri"]))
    
    ref_uris = [str(point["ref_uri"]).strip("<>") for point in prop_dicts]
    ref_uris = [f"'{uri}'" for uri in ref_uris]

    time_series = TimeSeries(
        title=title,
        dataSource="grafana-postgresql-datasource-1",
        targets=[
            SqlTarget(rawSql=f"SELECT date_trunc('minute',ts) as time, REPLACE(point_uri, 'urn:acquirium#','') as label, AVG(CAST(value AS decimal )) AS value FROM timeseries WHERE point_uri IN ( {','.join(ref_uris)} ) GROUP BY time, label ORDER BY time DESC LIMIT 1000;", format="table")
        ],
        gridPos=GridPos(h=height, w=width, x=x, y=y),
        transformations=[{
          "id": "partitionByValues",
          "options": {
            "fields": [
              "label"
            ],
            "keepFields": False,
            "naming": {
              "asLabels": True
            }
          }
        }]
    )
    return time_series