import csv
from datalog import DataLogReader


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
        with open('logs.csv', 'w', newline='') as csvfile:
            csv_writer = csv.writer(csvfile, delimiter=',',
                                    quotechar='|', quoting=csv.QUOTE_MINIMAL)
            for record in reader:
                timestamp = record.timestamp / 1000000
                if record.isStart():
                    try:
                        data = record.getStartData()
                        print(
                            f"Start({data.entry}, name='{data.name}', type='{data.type}', metadata='{data.metadata}') [{timestamp}]"
                        )
                        if data.entry in entries:
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
                    print(f"Data({record.entry}, size={len(record.data)}) ", end="")
                    entry = entries.get(record.entry)
                    if entry is None:
                        print("<ID not found>")
                        continue
                    print(f"<name='{entry.name}', type='{entry.type}'> [{timestamp}]")

                    try:
                        # handle systemTime specially
                        if entry.name == "systemTime" and entry.type == "int64":
                            dt = datetime.fromtimestamp(record.getInteger() / 1000000)
                            print("  {:%Y-%m-%d %H:%M:%S.%f}".format(dt))
                            continue

                        if entry.type == "double":
                            value = record.getDouble()
                            print(f"  {value}")
                            csv_writer.writerow([entry.name, entry.type, value])
                        elif entry.type == "int64":
                            value = record.getInteger()
                            print(f"  {record.getInteger()}")
                            csv_writer.writerow([entry.name, entry.type, value])
                        elif entry.type in ("string", "json"):
                            value = record.getString()
                            print(f"  '{record.getString()}'")
                            csv_writer.writerow([entry.name, entry.type, value])
                        elif entry.type == "msgpack":
                            value = record.getMsgPack()
                            print(f"  '{record.getMsgPack()}'")
                            csv_writer.writerow([entry.name, entry.type, value])
                        elif entry.type == "boolean":
                            value = record.getBoolean()
                            print(f"  {record.getBoolean()}")
                            csv_writer.writerow([entry.name, entry.type, value])
                        elif entry.type == "boolean[]":
                            arr = record.getBooleanArray()
                            csv_writer.writerow([entry.name, entry.type, arr])
                            print(f"  {arr}")
                        elif entry.type == "double[]":
                            arr = record.getDoubleArray()
                            csv_writer.writerow([entry.name, entry.type, arr])
                            print(f"  {arr}")
                        elif entry.type == "float[]":
                            arr = record.getFloatArray()
                            print(f"  {arr}")
                            csv_writer.writerow([entry.name, entry.type, arr])
                        elif entry.type == "int64[]":
                            arr = record.getIntegerArray()
                            print(f"  {arr}")
                            csv_writer.writerow([entry.name, entry.type, arr])
                        elif entry.type == "string[]":
                            arr = record.getStringArray()
                            print(f"  {arr}")
                            csv_writer.writerow([entry.name, entry.type, arr])
                    except TypeError as e:
                        print("  invalid")
