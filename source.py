import datetime
import json
import sqlite3
import sys

# If session end's up in 30 minutes (we check time between the FIRST 'visit_in' and CURRENT 'visit_in'
# and if it's more than 30 minutes we start new session), than USE_FIRST must be set to True
# else (we check time between LAST 'visit_in' and CURRENT 'visit_in' and if it's more than 30 minutes we
# start new session), than USE_FIRST must be set to False
USE_FIRST = True

"""
Because JSON is dirty (a lot of trailing commas, some brackets don't closed) we need to parse it line by line.
Let's accept as the fact that a separate line corresponds to a separate record.
"""


class SessionManager:

    def __init__(self, log_file_path, result_file_path, db_file_path):
        self.connection = sqlite3.connect(db_file_path)
        self.connection.row_factory = sqlite3.Row
        self.cursor = self.connection.cursor()
        self._create_table()
        self._create_indexes()
        self.log_file_path = log_file_path
        self.result_file_path = result_file_path

    def _create_table(self):
        self.cursor.execute(
            """CREATE TABLE IF NOT EXISTS sessions  (
                crc INTEGER NOT NULL,
                client INTEGER NOT NULL,
                elite INTEGER,
                visit_in INTEGER NOT NULL,
                time TEXT,
                duration INTEGER NOT NULL
                )
            """)

    def _create_indexes(self):
        self.cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_crc ON sessions(crc)
        """)
        self.cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_crc_visit ON sessions(crc, visit_in)
        """)

    @staticmethod
    def _get_structured_visit_record(visit_string):
        visit_string = visit_string.rstrip(',\n ]').lstrip('[')
        if visit_string[-1] is not '}':
            visit_string += '}'
        visit_record = json.loads(visit_string)
        if 'elite' not in visit_record:
            visit_record['elite'] = None
        if 'time' not in visit_record:
            visit_record['time'] = None
        visit_record['duration'] = 15 * 60
        return visit_record

    def create_new_session(self, session_data):
        self.cursor.execute("""INSERT INTO sessions (crc,client,elite,visit_in,time,duration) 
            VALUES (:crc,:client,:elite,:visit_in,:time,:duration)""", session_data)

    def get_last_session(self, user_crc):
        return self.cursor.execute("""SELECT visit_in, ROWID FROM sessions
                                   WHERE crc=? ORDER BY visit_in DESC LIMIT 1""", (user_crc,)).fetchone()

    def update_last_session(self, new_duration, session_id, new_visit_in=None):
        if not new_visit_in:
            self.cursor.execute("""UPDATE sessions SET duration=duration+? WHERE ROWID=?""",
                                (new_duration, session_id))
        else:
            self.cursor.execute("""UPDATE sessions SET duration=duration+?, visit_in=? WHERE ROWID=?""",
                                (new_duration, new_visit_in, session_id))

    def parse_sessions(self):
        with open(self.log_file_path, 'r') as log_file:
            for visit_string in log_file:
                visit_record = SessionManager._get_structured_visit_record(visit_string)
                last_session = self.get_last_session(user_crc=visit_record['crc'])
                if not last_session or visit_record['visit_in'] - last_session['visit_in'] > 30 * 60:
                    self.create_new_session(visit_record)
                else:
                    time_between_visits = visit_record['visit_in'] - last_session['visit_in']
                    if USE_FIRST:
                        self.update_last_session(
                            new_duration=time_between_visits,
                            session_id=last_session['ROWID'])
                    else:
                        self.update_last_session(
                            new_duration=time_between_visits,
                            session_id=last_session['ROWID'],
                            new_visit_in=visit_record['visit_in'])

    def dump_to_json(self):
        query_result = self.cursor.execute("""SELECT crc, client, elite, time, duration FROM sessions""")
        with open(self.result_file_path, 'w') as result_file:
            result_file.write('[')
            result_file.write(json.dumps(dict(next(query_result))))
            for row in query_result:
                result_file.write(',\n')
                result_file.write(json.dumps(dict(row)))
            result_file.write(']')

    def get_user_sessions_amount(self, user_crc):
        return self.cursor.execute("""SELECT COUNT(*) FROM sessions
                                           WHERE crc=?""", (user_crc,)).fetchone()[0]

    def get_user_time_on_site(self, user_crc, is_formatted=True):
        user_time = self.cursor.execute("""SELECT SUM(duration) FROM sessions
                                           WHERE crc=?""", (user_crc,)).fetchone()[0]
        if is_formatted:
            user_time = datetime.timedelta(seconds=user_time)
        return user_time

    def __del__(self):
        self.connection.commit()
        self.connection.close()


if __name__ == '__main__':
    worker = SessionManager(sys.argv[1], sys.argv[2], sys.argv[3])
    worker.parse_sessions()
    worker.dump_to_json()
    # print(worker.get_user_sessions_amount(3953986574))
    # print(worker.get_user_time_on_site(3953986574))
