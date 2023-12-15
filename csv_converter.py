import csv
from datalog import DataLogReader
import gzip
from datetime import datetime
import mmap
import sys

def csv_convert(file_name):
     with open(file_name, "r") as f:
        mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        reader = DataLogReader(mm)
        if not reader:
            print("not a log file", file=sys.stderr)
            sys.exit(1)

        entries = {}
        input_log = sys.argv[1]
        pos = input_log.rfind(".")
        output_csv = input_log[:pos] + ".csv"        
        with open(output_csv, 'w+', newline='') as csvfile:
            csv_writer = csv.writer(csvfile, delimiter=',',
                                    quotechar='|', quoting=csv.QUOTE_MINIMAL)
            for record in reader:
                timestamp = record.timestamp / 1000000
                if record.isStart():
                    try:
                        data = record.getStartData()
                        entries[data.entry] = data
                    except TypeError as e:
                        print("Start(INVALID)")
                elif record.isFinish():
                    try:
                        entry = record.getFinishEntry()
                        print(f"Finish({entry}) [{timestamp}]")
                        if entry not in entries:
                            print("...ID not found")
                        else:
                            del entries[entry]
                    except TypeError as e:
                        print("Finish(INVALID)")
                elif record.isSetMetadata():
                    try:
                        data = record.getSetMetadataData()
                        print(f"SetMetadata({data.entry}, '{data.metadata}') [{timestamp}]")
                        if data.entry not in entries:
                            print("...ID not found")
                    except TypeError as e:
                        print("SetMetadata(INVALID)")
                elif record.isControl():
                        print("Unrecognized control record")
                else:
                    entry = entries.get(record.entry)
                    if entry is None:
                        print("<ID not found>")
                        continue
                    try:
                        # handle systemTime specially
                        if entry.name == "systemTime" and entry.type == "int64":
                            dt = datetime.fromtimestamp(record.getInteger() / 1000000)
                            continue

                        if entry.type == "double":
                            value = record.getDouble()
                            csv_writer.writerow([entry.name, entry.type, value, record.timestamp])
                        elif entry.type == "int64":
                            value = record.getInteger()
                            csv_writer.writerow([entry.name, entry.type, value, record.timestamp])
                        elif entry.type in ("string", "json"):
                            value = record.getString()
                            value = value.replace("\r", " ")
                            value = value.replace("\n", " ")
                            csv_writer.writerow([entry.name, entry.type, value, record.timestamp])
                        elif entry.type == "msgpack":
                            value = record.getMsgPack()
                            csv_writer.writerow([entry.name, entry.type, value, record.timestamp])
                        elif entry.type == "boolean":
                            value = record.getBoolean()
                            csv_writer.writerow([entry.name, entry.type, value, record.timestamp])
                        elif entry.type == "boolean[]":
                            arr = record.getBooleanArray()
                            csv_writer.writerow([entry.name, entry.type, arr, record.timestamp])
                        elif entry.type == "double[]":
                            arr = record.getDoubleArray()
                            csv_writer.writerow([entry.name, entry.type, arr, record.timestamp])
                        elif entry.type == "float[]":
                            arr = record.getFloatArray()
                            csv_writer.writerow([entry.name, entry.type, arr, record.timestamp])
                        elif entry.type == "int64[]":
                            arr = record.getIntegerArray()
                            csv_writer.writerow([entry.name, entry.type, arr, record.timestamp])
                        elif entry.type == "string[]":
                            arr = record.getStringArray()
                            csv_writer.writerow([entry.name, entry.type, arr, record.timestamp])
                    except TypeError as e:
                        print("  invalid")
                    except UnicodeEncodeError:
                        print(arr)
                        print("   UnicodeEncodeError")

        f_in = open(output_csv)
        f_out = gzip.open(output_csv[:-3]+"gz", 'wt')
        f_out.writelines(f_in)
        f_out.close()
        f_in.close()
        print("--Complete--")

if __name__ == "__main__":

    if len(sys.argv) != 2:
        print("Usage: csv_converter.py <file>", file=sys.stderr)
        sys.exit(1)

    csv_convert(sys.argv[1])
