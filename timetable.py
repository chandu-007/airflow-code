from datetime import timedelta
from typing import Optional
from pendulum import Date, DateTime, Time, timezone
from airflow.plugins_manager import AirflowPlugin
from airflow.timetables.base import DagRunInfo, DataInterval, TimeRestriction, Timetable
from bisect import bisect
from datetime import datetime
import pendulum
from ast import literal_eval
import json



class run_calendars(Timetable):
    def __init__(self, args) -> None:
        self.args=args
        dates=args.get('dates')
        times=args.get('times')
        self.tz=args.get('tz','UTC')
        tz=timezone(args.get('tz','UTC'))
        dates=[datetime.strptime(date_string.strip(),"%m/%d/%Y %H:%M:%S").replace(tzinfo=tz) for date_string in dates]
        times=[Time(i,j) for i,j in times]
        self.run_times=[]
        if times:
            for date in dates:
                for time in times:
                    self.run_times.append(DateTime.combine(date,time).replace(tzinfo=tz))
        else:
            self.run_times=dates
        self.run_times=list(set(self.run_times))
        self.run_times.sort()



    def infer_manual_data_interval(self, run_after: DateTime) -> DataInterval:
        index_=bisect(self.run_times,run_after)
        if index_>1:
            next_start=self.run_times[index_-2]
            next_end=self.run_times[index_-1]
        elif index_>0:
            next_end=self.run_times[index_-1]
            next_start=next_end-timedelta(days=1)
        else:
            next_end=run_after
            next_start=next_end-timedelta(days=1)
        return DataInterval(start=next_start, end=next_end)






    def next_dagrun_info(self,*,last_automated_data_interval: Optional[DataInterval],
                                            restriction: TimeRestriction,)-> Optional[DagRunInfo]:


        if last_automated_data_interval is not None:

            end=last_automated_data_interval.end
            now=pendulum.now(self.tz)
            next_start=end

            index_=bisect(self.run_times,now)

            if self.run_times[index_-1]==end:
                if index_>len(self.run_times):
                    return None
                
                next_end=self.run_times[index_]
            else:
    
                next_end=self.run_times[index_-1]

        else:
            next_start = restriction.earliest
            if next_start is None:
                return None


            if not restriction.catchup:
                index_=bisect(self.run_times,pendulum.now(self.tz))
                if index_>0:
                    index_-=1
                next_end=self.run_times[index_]
            else:
                next_end=self.run_times[0]

        if restriction.latest is not None and next_start > restriction.latest :
              return None

        return DagRunInfo.interval(start=next_start, end=next_end)

    def serialize(self):
        return {"args": json.dumps(self.args)}

    @classmethod
    def deserialize(cls, value) -> Timetable:
        return cls(literal_eval(value["args"]))



class automated(AirflowPlugin):
    name = "automated"
    timetables = [run_calendars]
