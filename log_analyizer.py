import os
import pandas as pd
import sys
from sqlite3 import connect 


if __name__ == "__main__":

    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <file>", file=sys.stderr)
        sys.exit(1)

    colnames=['metric', 'data_type', 'value', 'timestamp']
    df = pd.read_csv(sys.argv[1], quotechar='|', header=None, names=colnames)

    build_date = df.loc[df['metric'] == '/RealMetadata/BuildDate', 'value'].values[0]
    robot = df.loc[df['metric'] == '/RealMetadata/Robot', 'value'].values[0]
    runtime_type = df.loc[df['metric'] == '/RealMetadata/RuntimeType', 'value'].values[0]
    git_branch = df.loc[df['metric'] == '/RealMetadata/GitBranch', 'value'].values[0]
    project_name = df.loc[df['metric'] == '/RealMetadata/ProjectName', 'value'].values[0]
    log_filename = os.path.basename(sys.argv[1])

    log_metadata = [[log_filename, build_date, robot, runtime_type, git_branch, project_name]]

    df_log_metadata = pd.DataFrame(log_metadata, columns=['LogFileName', 'BuildDate', 'Robot', 'RuntimeType', 'GitBranch', 'ProjectName'])

    df_boolean_log_data = df[df['data_type'] == 'boolean']
    df_boolean_log_data['boolean_value'] = df_boolean_log_data['value'].astype(bool)
    df_numerical_log_data = df[(df['data_type'] == 'int64') | (df['data_type'] == 'double') | (df['data_type'] == 'float')]
    df_numerical_log_data['numeric_value'] = pd.to_numeric(df_numerical_log_data['value'])
    df_other_log_data = df[(df['data_type'] != 'boolean') & (df['data_type'] != 'int64') & (df['data_type'] != 'double') & (df['data_type'] != 'float')]

    df_log_data = pd.concat([df_boolean_log_data, df_numerical_log_data, df_other_log_data])
    df_log_data['timestamp'] = df_log_data['timestamp'].astype('int64')
    df_log_data['log_file_name'] = log_filename

    print(df_log_data)

    conn = connect("log_db.db")
    curr = conn.cursor()

    curr.execute('CREATE TABLE IF NOT EXISTS log_metadata (LogFileName TEXT PRIMARY KEY, BuildDate TEXT, Robot TEXT, RuntimeType TEXT, GitBranch TEXT, ProjectName TEXT)')
    conn.commit()

    sql_query = pd.read_sql("SELECT * FROM log_metadata where LogFileName = '" + log_filename + "'", conn)
    df_log_metadata_temp = pd.DataFrame(sql_query)
    print(df_log_metadata_temp)

    if df_log_metadata_temp.size == 0:
        df_log_metadata.to_sql('log_metadata', conn, if_exists='append', index=False)
        curr.execute('CREATE TABLE IF NOT EXISTS log_data (metric TEXT, data_type TEXT, value TEXT, timestamp INTEGER, boolean_value INTEGER, numeric_value REAL, log_file_name TEXT)')
        conn.commit()
        df_log_data.to_sql('log_data', conn, if_exists='append', index=False)