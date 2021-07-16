import pandas as pd
import re
import os
import time
import logging
import numpy as np
from os import listdir
from os.path import isfile, join


def parse(filepath):
    start = time.time()
    df = pd.DataFrame()  # new temp data frame for each file
    with open(filepath, encoding="utf-16") as f:
        for index, line in enumerate(f):
            format_line = line.strip()

            if format_line.startswith("================================================================"):
                event_dict = {}  # storing each event in dictionary - reset on new event

                event_info = next(f).strip()  # Event info (line need parsing)
                event = event_info.split()
                event_dict["event_start_day"], event_dict["event_start_date"], event_dict["event_start_time"],\
                event_dict["event"], event_dict["id"], event_dict["event_finish_day"], event_dict["event_finish_date"],\
                event_dict["event_finish_time"] = event[0], ' '.join(event[1:4]), event[4], event[5], event[6].replace(
                    "ID=", ""), event[9], ' '.join(event[10:13]), event[13]

                event_league, event_name = f.readline().strip(), f.readline().strip()
                event_dict['event_league'], event_dict['event_name'] = event_league, event_name

            if format_line.startswith("--------------------"):
                market_dict = {}  # new dictionary each time tag detected
                market_info = next(f).strip()
                market_dict['market_info'] = market_info  # market name

            if " *" in format_line:
                outcome = re.sub(r"\s\s+", "-----", format_line).split("-----")[0]  # market outcome (selection)
                rng_value = format_line.split(" ")[-2]  # random number generated
                price = re.sub(r"\s\s+", "-----", format_line).split("-----")[1]  # odds

                if "*" in price:  # set values with * (wrong rng value in price cell) to null
                    price = np.nan

                market_dict['outcome'], market_dict['rng_value'], market_dict['price'] = outcome, rng_value, price

                output_dict = {}
                output_dict.update(event_dict)
                output_dict.update(market_dict)

                df = df.append(output_dict, ignore_index=True)
        end = time.time()
        time_taken = round(end - start, 2)
        return df, time_taken


def script_log(module_name):
    logging.basicConfig(filename="log.log", level=logging.INFO,
                        format='[%(asctime)s]{%(name)s:%(lineno)d} %(levelname)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')  # file logging
    logger = logging.getLogger(module_name)
    handler = logging.StreamHandler()  # console logging
    formatter = logging.Formatter("[%(asctime)s]{%(name)s:%(lineno)d} %(levelname)s - %(message)s", "%Y-%m-%d %H:%M:%S")
    handler.setFormatter(formatter)
    logger.setLevel(logging.INFO)  # set log level
    if logger.hasHandlers():  # stop duplicate handlers
        logger.handlers.clear()
    logger.addHandler(handler)
    return logger


def main():
    logger = script_log(__name__)
    final_output = pd.DataFrame()  # storage data frame
    dir_name = os.path.dirname(__file__)  # parse through input file directory for each txt
    file_path = os.path.join(dir_name, 'input')

    file_list = [f for f in listdir(file_path) if isfile(join(file_path, f))]
    file_list = [x for x in file_list if ".txt" in x]  # only .txt
    logger.info(f"Found {len(file_list)} .txt files")

    for file in file_list:
        complete_path = os.path.join(file_path, file)
        logger.info(f"Parsing: {complete_path}, ({file_list.index(file) + 1}/{len(file_list)}-"
              f"{int(((file_list.index(file) + 1) / len(file_list)) * 100)}%)")
        df_out, time_taken = parse(filepath=complete_path)
        per_row_time = round(time_taken / len(df_out.index), 2)
        logger.info(f"File parsed, saved {len(df_out.index)} rows, took {time_taken} seconds, {per_row_time} per row")
        final_output = final_output.append(df_out, ignore_index=True)
    logger.info(f"Saving output to csv, saved {len(final_output.index)} rows")
    final_output.to_csv("output.csv", encoding="utf-16", sep=",", index=False)


if __name__ == '__main__':
    main()
