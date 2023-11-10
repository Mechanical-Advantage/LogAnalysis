import os
import pandas as pd
import sys

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
    print(df_log_metadata)