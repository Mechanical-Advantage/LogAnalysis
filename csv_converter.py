import csv
from datalog import DataLogReader
import gzip

SP = False # should print?

if __name__ == "__main__":
    from datetime import datetime
    import mmap
    import sys

    if len(sys.argv) != 2:
        print("Usage: csv_converter.py <file>", file=sys.stderr)
        sys.exit(1)

    with open(sys.argv[1], "r") as f:
        mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        reader = DataLogReader(mm)
        if not reader:
            print("not a log file", file=sys.stderr)
            sys.exit(1)

        entries = {}
        input_log = sys.argv[1]
        pos = input_log.rfind(".")
        global output_csv
        output_csv = input_log[:pos] + ".csv"        
        with open(output_csv, 'w+', newline='') as csvfile:
            csv_writer = csv.writer(csvfile, delimiter=',',
                                    quotechar='|', quoting=csv.QUOTE_MINIMAL)
            for record in reader:
                timestamp = record.timestamp / 1000000
                if record.isStart():
                    try:
                        data = record.getStartData()
                        if SP:
                            print(
                            f"Start({data.entry}, name='{data.name}', type='{data.type}', metadata='{data.metadata}') [{timestamp}]"
                        )
                        if data.entry in entries:
                            if SP: 
                                print("...DUPLICATE entry ID, overriding")
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
                    if SP:
                        print(f"Data({record.entry}, size={len(record.data)}) ", end="")
                    entry = entries.get(record.entry)
                    if entry is None:
                        print("<ID not found>")
                        continue
                    if SP: print(f"<name='{entry.name}', type='{entry.type}'> [{timestamp}]")

                    try:
                        # handle systemTime specially
                        if entry.name == "systemTime" and entry.type == "int64":
                            dt = datetime.fromtimestamp(record.getInteger() / 1000000)
                            if SP:
                                print("  {:%Y-%m-%d %H:%M:%S.%f}".format(dt))
                            continue

                        if entry.type == "double":
                            value = record.getDouble()
                            if SP:
                                print(f"  {value}")
                            csv_writer.writerow([entry.name, entry.type, value, record.timestamp])
                        elif entry.type == "int64":
                            value = record.getInteger()
                            if SP:
                                print(f"  {record.getInteger()}")
                            csv_writer.writerow([entry.name, entry.type, value, record.timestamp])
                        elif entry.type in ("string", "json"):
                            value = record.getString()
                            if SP:
                                print(f"  '{record.getString()}'")
                            csv_writer.writerow([entry.name, entry.type, value, record.timestamp])
                        elif entry.type == "msgpack":
                            value = record.getMsgPack()
                            if SP:
                                print(f"  '{record.getMsgPack()}'")
                            csv_writer.writerow([entry.name, entry.type, value, record.timestamp])
                        elif entry.type == "boolean":
                            value = record.getBoolean()
                            if SP:
                                print(f"  {record.getBoolean()}")
                            csv_writer.writerow([entry.name, entry.type, value, record.timestamp])
                        elif entry.type == "boolean[]":
                            arr = record.getBooleanArray()
                            csv_writer.writerow([entry.name, entry.type, arr, record.timestamp])
                            if SP:
                                print(f"  {arr}")
                        elif entry.type == "double[]":
                            arr = record.getDoubleArray()
                            csv_writer.writerow([entry.name, entry.type, arr, record.timestamp])
                            if SP:
                                print(f"  {arr}")
                        elif entry.type == "float[]":
                            arr = record.getFloatArray()
                            if SP:
                                print(f"  {arr}")
                            csv_writer.writerow([entry.name, entry.type, arr, record.timestamp])
                        elif entry.type == "int64[]":
                            arr = record.getIntegerArray()
                            if SP:  
                                print(f"  {arr}")
                            csv_writer.writerow([entry.name, entry.type, arr, record.timestamp])
                        elif entry.type == "string[]":
                            arr = record.getStringArray()
                            if SP:
                                print(f"  {arr}")
                            csv_writer.writerow([entry.name, entry.type, arr, record.timestamp])
                    except TypeError as e:
                        print("  invalid")
                    except UnicodeEncodeError:
                        print(arr)
                        print("   UnicodeEncodeError")

    f_in = open(output_csv)
    f_out = gzip.open(output_csv[:-3]+"gz", 'wb')
    # TODO: Write csv to gzip file
    print("--Complete--")