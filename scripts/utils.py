from datetime import datetime

def format_timestamp_to_file_name(timestamp):
    return timestamp.strftime('%Y-%m-%d_%H-%M-%S')

def parse_custom_timestamp(timestamp):
    return datetime.strptime(timestamp, '%Y-%m-%d_%H-%M-%S')