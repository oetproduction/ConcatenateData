'''
Collect data from dive_reports/sampled files into a single 
CSV or other data structure. 
'''

import argparse
import re
import logging
import os
import csv
from datetime import datetime
from datetime import timedelta

logging.basicConfig(
    format='%(levelname)s %(funcName)s %(lineno)s: %(message)s', 
    level=logging.DEBUG)

def parse_cli_args():
    parser = argparse.ArgumentParser(description=__doc__)

    parser.add_argument(
        'sampled_files',
        nargs='+',
        help='TSV 1 second files from dive_reports/sampled')

    parser.add_argument(
        '--dvl-dive-report',
        '-d',
        help='DVL data file from parse_navest.py. Tab delimited columns '
            'with time, lat, lon, depth')

    parser.add_argument(
        '--sealog-csv',
        '-s',
        help='sealog-herc JSON file')

    parser.add_argument(
        '--csv-to',
        '-c',
        help='Path of output CSV file')

    return parser.parse_args()



def main(args):

    sampled_file_formats = [
        {
            'sensor': 'ctd',
            'file_pattern': re.compile(r'.*\.CTD\.sampled\.tsv$'),
            'cols': [
                'time',
                'temp_c', 
                'conductivity', 
                'pressure_psi', 
                'salinity_psu', 
                'sound_velocity_ms']
        },
        {
            'sensor': 'paro',
            'file_pattern': re.compile(r'.*\.DEP1\.sampled\.tsv$'),
            'cols': [
                'time',
                'depth_m']
        },
        {
            'sensor': 'usbl',
            'file_pattern': re.compile(r'.*\.NAV\.M1\.sampled\.tsv$'),
            'cols': [
                'time',
                'lat',
                'lon'] ## last field is depth, which we ignore
        },
        {
            'sensor': 'oxygen_uncompensated',
            'file_pattern': re.compile(r'.*\.O2S\.sampled\.tsv$'),
            'cols': [
                'time',
                'concentration_micromolar',
                'saturation_percent'] ## ignoring last field, temperature
        }
    ]


    parsers = []
    for sampled_path in args.sampled_files:
        sampled_name = os.path.basename(sampled_path)
        for file_format in sampled_file_formats:
            if not file_format['file_pattern'].match(sampled_name):
                continue

            parser_iter = read_file(sampled_path)
            parser_iter = parse_sampled_lines(parser_iter, file_format)
            parsers.append(parser_iter)


    if args.dvl_dive_report is not None:
        parser_iter = read_file(args.dvl_dive_report)
        parser_iter = parse_dvl_dive_report(parser_iter)
        parser_iter = truncate_time_to_seconds(parser_iter)
        parsers.append(parser_iter)

    if args.sealog_csv is not None:
        parser_iter = read_csv_file(args.sealog_csv)
        parser_iter = remove_matching(parser_iter, 'event_free_text', '')
        parser_iter = keep_only_fields(parser_iter, ['ts', 'event_free_text'])
        parser_iter = rename_field(parser_iter, 'ts', 'time')
        parser_iter = rename_field(parser_iter, 'event_free_text', 'sealog_event_free_text')
        parser_iter = truncate_time_to_seconds(parser_iter)
        parser_iter = extend_sealog_messages(parser_iter)
        parsers.append(parser_iter)

    merged_data = merge_data(parsers)

    first_time = min([key for key in merged_data])
    last_time = max([key for key in merged_data])
    time_iter = generate_time_sequence(
        start=first_time, 
        end=last_time, 
        interval_seconds=5)

    if args.csv_to is not None:
        write_csv(merged_data, args.csv_to, time_iter)


def read_file(path):
    with open(path) as in_file:
        for line in in_file:
            yield line


def read_csv_file(path):
    with open(path) as in_file:
        reader = csv.DictReader(in_file)
        for row in reader:
            yield row

def remove_matching(data_iter, key, value):
    for item in data_iter:
        if item[key] == value:
            continue

        yield item

def keep_only_fields(data_iter, field_names):
    for item in data_iter:
        new_item = {}
        for field_name in field_names:
            new_item[field_name] = item[field_name]

        yield new_item

def rename_field(data_iter, old_name, new_name):
    for item in data_iter:
        item[new_name] = item.pop(old_name)
        yield item


def extend_sealog_messages(data_iter):
    '''
    Duplicate sealog messages for every second until the next message
    '''
    for item in data_iter:
        last_item_time = datetime.fromisoformat(item['time'])
        last_item = item
        yield item
        break

    for item in data_iter:
        next_item_time = datetime.fromisoformat(item['time'])
        new_item_time = last_item_time
        while new_item_time <= next_item_time:
            new_item_time += timedelta(seconds=1)
            new_item = last_item.copy()
            new_item['time'] = new_item_time.strftime('%Y-%m-%dT%H:%M:%S')
            yield new_item

        last_item = item
        last_item_time = datetime.fromisoformat(item['time'])
        yield item



def parse_sampled_lines(lines, file_format):
    for line in lines:
        data_record = {}
        fields = line.split('\t')
        for i, column in enumerate(file_format['cols']):

            ## time is a shared key between all sensors, which lets us merge them
            ## into the same record. All other keys get prefixed with the sensor
            ## name to avoid name collisions
            if column == 'time':
                key = 'time'
            else:
                key = '{}_{}'.format(file_format['sensor'], column)

            # all_keys.add(key)
            data_record[key] = fields[i].strip()

        yield data_record


def parse_dvl_dive_report(data_iter):
    for line in data_iter:
        fields = line.split('\t')
        record = {
            'time': fields[0],
            'dvl_lat': fields[1],
            'dvl_lon': fields[2]}

        yield record


def truncate_time_to_seconds(data_iter):
    ## bit of a hack to subsample dvl data to full seconds
    ## by matching the first 19 characters of the time string
    last_full_timestamp = None
    for record in data_iter:
        full_second_time = record['time'][:19]
        if full_second_time == last_full_timestamp:
            continue

        last_full_timestamp = full_second_time
        record['time'] = full_second_time
        yield record


def generate_time_sequence(start, end, interval_seconds):
    '''
    all input and output timestamps are strings 
    in the format 2023-10-28T19:00:04
    '''
    start_time = datetime.fromisoformat(start)
    end_time = datetime.fromisoformat(end)

    ## start on the minute
    start_time = start_time.replace(second=0, minute=start_time.minute+1)

    max_seconds = (end_time - start_time).total_seconds()

    for i in range(0, int(max_seconds), interval_seconds):
        result_time = start_time + timedelta(seconds=i)
        yield result_time.strftime('%Y-%m-%dT%H:%M:%S')


def merge_data(parsers):
    merged_data = {}
    for parser in parsers:
        for data_record in parser:
            time_key = data_record['time']
            try:
                ## merge records by time if it already exists
                merged_data[time_key] = {**merged_data[time_key], **data_record}
            except KeyError:
                ## add record for the time one doesn't already exist
                merged_data[time_key] = data_record

    return merged_data



def write_csv(data, out_path, time_iter):
    ## inefficiently make sure we have all the dict keys used ever
    all_keys = set()
    for time_key in data:
        all_keys.update([key for key in data[time_key]])

    sorted_keys = sorted(list(all_keys))
    ## put time first, for convenience
    sorted_keys = [key for key in sorted_keys if key != 'time']
    sorted_keys.insert(0, 'time')

    with open(out_path, 'w', newline='') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=sorted_keys, delimiter='\t')
        writer.writeheader()

        for timestamp in time_iter:
            try:
                writer.writerow(data[timestamp])
            except KeyError as err:
                logging.warning('No record for {}'.format(err))


if __name__ == '__main__':
    cli_args = parse_cli_args()
    main(cli_args)