import sys
from enum import Enum
import extract
import transform
import pandas as pd


class DB_CHOOSING(Enum):
    test = "test"
    original = "orig"


MAX_COUNT_ARGUMENTS = 4
MIN_COUNT_ARGUMENTS = 1
FIRST_ARGUMENT = 1
SECOND_ARGUMENT = 2
THIRD_ARGUMENT = 3
COUNT_OF_DATA = 100


def main():

    if len(sys.argv) > MIN_COUNT_ARGUMENTS:
        first_argument = sys.argv[FIRST_ARGUMENT]

        if first_argument == "--help":
            print(
                "========================================================================="
            )
            print("It is a help command:")
            print(
                "If you want only to download raw data from google drive, put 'ext' as first argument and 'FILE_ID' as second argument"
            )
            print(
                "If you want to download, transform data and write them to your db, put 'all' as first argument, 'FILE_ID' as second argument, and 'test' or 'orig' as third argument"
            )
            print(
                "NOTE: Raw data will be downloaded in every situation and saved as: RAW_DATA.csv"
            )
            print("NOTE: Processed data will be saved as: PROCESSED_DATA.parquet")
            print(
                "========================================================================="
            )
            return 0

        if first_argument == "ext":
            if len(sys.argv) == SECOND_ARGUMENT + 1:
                print("Only extracting started")
                file_id = sys.argv[SECOND_ARGUMENT]
                df = extract.extract_data(file_id, save_name="RAW_DATA.csv")
                if df is None:
                    print("Failure")
                    return None
                print("Extraction finished successfully")
                return 5
            else:
                print(
                    "Error: not all arguments were passed, check python3 main.py --help"
                )
                return None

        if first_argument == "all":
            if len(sys.argv) == THIRD_ARGUMENT + 1:
                print(
                    "All stages of data transformation started. Make sure virtual env and dependencies are set up."
                )
                file_id = sys.argv[SECOND_ARGUMENT]

                # 1. Extract
                df = extract.extract_data(file_id, save_name="RAW_DATA.csv")
                if df is None:
                    print("Failure")
                    return None

                # 2. Transform
                df = transform.transform_data(df)
                df_reduced = df.head(COUNT_OF_DATA).copy()
                df_reduced.to_parquet("PROCESSED_DATA.parquet", index=False)
                print("Processed data saved as PROCESSED_DATA.parquet")

                # 3. Load
                third_argument = sys.argv[THIRD_ARGUMENT]
                if (
                    third_argument == DB_CHOOSING.test.value
                    or third_argument == DB_CHOOSING.original.value
                ):
                    transform.transform_data(df_reduced)
                    print("Everything has been written to the database.")
                    print(
                        "========================================================================="
                    )
                else:
                    print(
                        "Error: invalid database argument, check python3 main.py --help"
                    )
                    return None
            else:
                print(
                    "Error: not all arguments were passed, check python3 main.py --help"
                )
                return None

    else:
        print("No arguments have been passed, please write python3 main.py --help")


if __name__ == "__main__":
    main()
