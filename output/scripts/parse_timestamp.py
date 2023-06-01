import datetime
import sys

# Read the date argument from command-line
date_to_parse = sys.argv[1]
timestamp_format = "%d/%m/%Y  %H:%M:%S:%f"

def main():
# Parse the input string as a timestamp
    try:
        parsed_timestamp = datetime.datetime.strptime(date_to_parse, timestamp_format)
        t=parsed_timestamp.timestamp() * 1000
        print(int(t))
        return
    except ValueError:
        datetime.strptime('31/01/22 23:59:59.999999','%d/%m/%y %H:%M:%S.%f')
        print("Invalid input string or timestamp format.")

if __name__=='__main__':
    main()