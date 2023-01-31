import time
from sqlalchemy.orm import Session
from datetime import datetime

from modules.utilities.aws.rds.tables.log_event import LogEvent


def restore_logs(engine, logs):
    with Session(engine) as session:
        session.bulk_save_objects(logs)
        session.commit()


def complete_log(engine, start_time, overwrite, download, completed):
    new_log = get_new_log(start_time, overwrite, download, completed)
    with Session(engine) as session:
        session.add(LogEvent(**new_log))
        session.commit()


def get_new_log(start_time, overwrite, download, completed):
    return {
        'OverwriteTables': overwrite,
        'DownloadFiles': download,
        'RunTimeInSeconds': round(time.time() - start_time, 2),
        'StartTime': datetime.now(),
        'Completed': completed
    }
