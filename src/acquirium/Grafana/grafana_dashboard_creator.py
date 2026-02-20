import acquirium.Grafana.grafana_panel_creator as pc
from grafanalib.core import Dashboard, Time
from acquirium.Grafana.grafana_upload import upload_dashboard
from typing import TYPE_CHECKING


class GrafanaDashboardCreator:
    def __init__(self,title,tags,timezone='browser',refresh_interval='30s'):
        self.title = title
        self.dashboard = None
        self.panels_meta = {
            "gauge":[],
            "bar_chart":[],
            "state_timeline":[],
            "time_series":[]
        }
        self.panels = []
        self.refresh_interval = refresh_interval
        self.tags = tags
        self.timezone = timezone
    
    def add_gauge(self,prop_dict:dict):
        self.panels_meta["gauge"].append(prop_dict)


    def add_time_series(self,title:str,prop_dicts:list[dict]):            
        self.panels_meta["time_series"].append((title,prop_dicts))

    def generate_dashboard(self):

        import datetime
        latest_time = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        # print(self.panels_meta)
        num_gauges = len(self.panels_meta["gauge"])
        num_time_series = len(self.panels_meta["time_series"])

        x_offset = 0
        y_offset = 0
        g_width = 4
        g_height = 5
        t_width = 12
        t_height = 8
        for i in range(num_gauges):
            self.panels.append(pc.create_gauge(self.panels_meta["gauge"][i],height=g_height,width=g_width,x=x_offset,y=y_offset))
            x_offset += g_width
            if x_offset >= 24:
                x_offset = 0
                y_offset += g_height + 1
        
        y_offset += g_height + 1
        x_offset = 0
        for i in range(num_time_series):
            self.panels.append(pc.create_time_series(prop_dicts=self.panels_meta["time_series"][i][1],title=self.panels_meta["time_series"][i][0],height=t_height,width=t_width,x=x_offset,y=y_offset))
            x_offset += t_width
            if x_offset >= 24:  
                x_offset = 0
                y_offset += t_height + 1
        x_offset = 0
        y_offset += t_height + 1

        start_time = datetime.datetime.strptime(latest_time, "%Y-%m-%dT%H:%M:%S.%fZ") - datetime.timedelta(minutes=30)
        start_time = start_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        tmp =  datetime.datetime.strptime(latest_time, "%Y-%m-%dT%H:%M:%S.%fZ") + datetime.timedelta(minutes=10)
        latest_time = tmp.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


        dashboard = Dashboard(
            title=self.title,
            tags=self.tags,
            timezone=self.timezone,
            panels=self.panels,
            time=Time(start_time,latest_time)
        ).auto_panel_ids()
        
        self.dashboard = dashboard
    
    def upload_dashboard(self,server,api_key):
        upload_dashboard(self.dashboard,server,api_key)