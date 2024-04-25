import os
import pandas as pd
from supabase import create_client, Client
import re
import phonenumbers
from tqdm import tqdm
from xlsx2csv import Xlsx2csv
from io import StringIO
from openpyxl import load_workbook
from tenacity import retry, stop_after_attempt, wait_exponential
from dotenv import load_dotenv

load_dotenv()

url: str = "https://zhksyvctedgvaxuowfbb.supabase.co"
key: str = os.getenv("SUPABASE_KEY")
supabase: Client = create_client(url, key)


def read_excel_files(path: str, chunksize=1000):
    files = os.listdir(path)
    for f in tqdm(files, desc="Processing files"):
        if f.endswith(".xlsx"):
            file_path = os.path.join(path, f)
            buffer = StringIO()
            Xlsx2csv(file_path, outputencoding="utf-8").convert(buffer)
            buffer.seek(0)
            try:
                for chunk in pd.read_csv(buffer, low_memory=False, chunksize=chunksize):
                    yield chunk
            except Exception as e:
                print(f"Failed to read {f}: {e}")


def combine_columns(df, new_col, old_col):
    if new_col in df.columns and old_col in df.columns:
        df[new_col] = df[new_col].combine_first(df[old_col])
        df.drop(columns=[old_col], inplace=True)
        print(f"Successfully combined {new_col} and {old_col}")
    elif new_col in df.columns:
        print(f"Column {old_col} not found, keeping {new_col}")
    elif old_col in df.columns:
        print(f"Column {new_col} not found, renaming {old_col} to {new_col}")
        df.rename(columns={old_col: new_col}, inplace=True)
    else:
        print(f"Neither {new_col} nor {old_col} found in the DataFrame")


def split_name_columns(df, name_col, first_name_col, last_name_col):
    if name_col in df.columns:
        temp_df = df[name_col].str.split(",", expand=True)

        if temp_df.shape[1] == 1:
            # if there is only one column, then no comma was present in the entry
            temp_df[first_name_col] = pd.NA

        df[last_name_col] = temp_df[0].str.strip()
        df[first_name_col] = temp_df[1].str.strip()

        df.drop(columns=[name_col], inplace=True)
        print("Successfully split")
    else:
        print(f"Column {name_col} not found in the DataFrame")
    return df


column_mappings = [
    ("TOTAL_WORKERS", "TOTAL WORKERS"),
    ("H-1B_DEPENDENT", "H1B_DEPENDENT"),
    ("H-1B_DEPENDENT", "H_1B_DEPENDENT"),
    ("EMPLOYMENT_END_DATE", "END_DATE"),
    ("EMPLOYMENT_START_DATE", "START_DATE"),
    ("EMPLOYMENT_START_DATE", "BEGIN_DATE"),
    ("EMPLOYMENT_START_DATE", "PERIOD_OF_EMPLOYMENT_START_DATE"),
    ("NEW_CONCURRENT_EMPLOYMENT", "NEW_CONCURRENT_EMP"),
    ("NAICS_CODE", "NAIC_CODE"),
    ("EMPLOYER_ADDRESS1", "EMPLOYER_ADDRESS"),
    ("EMPLOYER_POC_ADDRESS_1", "EMPLOYER_POC_ADDRESS1"),
    ("EMPLOYER_POC_ADDRESS_2", "EMPLOYER_POC_ADDRESS2"),
    ("EMPLOYMENT_END_DATE", "END_DATE"),
    ("EMPLOYMENT_END_DATE", "PERIOD_OF_EMPLOYMENT_END_DATE"),
    ("PW_OTHER_SOURCE", "PW_OTHER_SOURCE_1"),
    ("PW_SURVEY_NAME", "PW_SURVEY_NAME_1"),
    ("PW_OES_YEAR", "PW_OES_YEAR_1"),
    ("PW_NON-OES_YEAR", "PW_NON-OES_YEAR_1"),
    ("PREVAILING_WAGE", "PREVAILING_WAGE_1"),
    ("PW_UNIT_OF_PAY", "PW_UNIT_OF_PAY_1"),
    ("WAGE_RATE_OF_PAY_FROM", "WAGE_RATE_OF_PAY_FROM_1"),
    ("WAGE_RATE_OF_PAY_TO", "WAGE_RATE_OF_PAY_TO_1"),
    ("PW_TRACKING_NUMBER", "PW_TRACKING_NUMBER_1"),
    ("PW_WAGE_LEVEL", "PW_WAGE_LEVEL_1"),
    ("PW_SURVEY_PUBLISHER", "PW_SURVEY_PUBLISHER_1"),
    ("SECONDARY_ENTITY", "SECONDARY_ENTITY_1"),
    ("SECONDARY_ENTITY_BUSINESS_NAME", "SECONDARY_ENTITY_BUSINESS_NAME_1"),
]


def combine_ops(df, mappings):
    df = split_name_columns(
        df,
        "AGENT_ATTORNEY_NAME",
        "AGENT_ATTORNEY_FIRST_NAME",
        "AGENT_ATTORNEY_LAST_NAME",
    )

    for old_col, new_col in mappings:
        combine_columns(df, old_col, new_col)
    return df


def custom_title_case(input_string):
    pattern = re.compile(r"\b(\d+)([a-zA-Z]+)\b")

    def replace_func(match):
        num_part = match.group(1)
        word_part = match.group(2)
        if word_part.lower() in ["th", "st", "nd", "rd"]:
            return num_part + word_part.lower()
        return num_part + word_part.capitalize()

    result = input_string.title()
    result = pattern.sub(replace_func, result)
    return result


def format_phone_number(number):
    try:
        parsed_number = phonenumbers.parse(number, "US")
        formatted_number = phonenumbers.format_number(
            parsed_number, phonenumbers.PhoneNumberFormat.E164
        )
        return formatted_number
    except phonenumbers.NumberParseException:
        return None


def map_to_parent(name, parent_companies):
    if pd.isna(name):
        return None
    for parent in parent_companies:
        if parent in name:
            return parent
    return None


def format_boolean(x):
    if pd.isna(x):
        return None
    if x in ["Y", "y", "Yes", "YES", "yes", "True", "TRUE", "true"]:
        return True
    if x in ["N", "n", "No", "NO", "no", "False", "FALSE", "false"]:
        return False
    return None


def clean_text_columns(df):
    title_keywords = ["ADDRESS", "CITY", "COUNTRY", "NAME", "PROVINCE", "TITLE"]
    additional_title_columns = [
        "STATUTORY_BASIS",
        "AGENT_REPRESENTING_EMPLOYER",
        "EMPLOYER_BUSINESS_DBA",
    ]
    title_text_columns = [
        col for col in df.columns if any(keyword in col for keyword in title_keywords)
    ]
    title_text_columns += [col for col in additional_title_columns if col in df.columns]
    for column in title_text_columns:
        if column in df.columns:
            df[column] = df[column].apply(
                lambda x: custom_title_case(str(x)) if isinstance(x, str) else x
            )

    lower_text_columns = [
        col for col in df.columns if "EMAIL_ADDRESS" in col or "EMAIL" in col
    ]
    for column in lower_text_columns:
        if column in df.columns:
            df[column] = df[column].apply(
                lambda x: x.lower() if isinstance(x, str) else x
            )

    upper_keywords = ["STATE", "INITIAL"]
    upper_text_columns = [
        col
        for col in df.columns
        if any(keyword in col for keyword in upper_keywords)
        and "NAME_OF_HIGHEST_STATE_COURT" != col
    ]
    for column in upper_text_columns:
        if column in df.columns:
            df[column] = df[column].apply(
                lambda x: x.upper() if isinstance(x, str) else x
            )


def clean_date_columns(df):
    date_columns = [
        col
        for col in df.columns
        if "DATE" in col or "YEAR" in col or col == "CASE_SUBMITTED"
    ]
    for col in date_columns:
        df[col] = (
            pd.to_datetime(df[col], errors="coerce", format="%Y-%m-%d")
            .dt.strftime("%Y-%m-%d")
            .where(pd.notnull(df[col]), None)
        )


def clean_phone_no_columns(df):
    phone_no_columns = [
        col for col in df.columns if "PHONE" in col and "PHONE_EXT" not in col
    ]
    for column in phone_no_columns:
        df[column] = df[column].apply(
            lambda x: format_phone_number(str(x)) if pd.notna(x) else None
        )


def clean_int_columns(df):
    int_keywords = ["WORKSITE_WORKERS", "TRACKING_NUMBER", "PHONE_EXT"]
    additional_int_columns = [
        "NAICS_CODE",
        "CHANGE_EMPLOYER",
        "CHANGE_PREVIOUS_EMPLOYMENT",
        "CONTINUED_EMPLOYMENT",
        "AMENDED_PETITION",
        "NEW_CONCURRENT_EMPLOYMENT",
        "PUBLIC_DISCLOSURE_LOCATION",
        "PW_SOURCE",
        "PW_SOURCE_OTHER",
        "TOTAL_WORKER_POSITIONS",
        "TOTAL_WORKSITE_LOCATIONS",
        "TOTAL_WORKERS",
    ]
    int_columns = [
        col
        for col in df.columns
        if any(keyword in col for keyword in int_keywords)
        or col in additional_int_columns
    ]
    for column in int_columns:
        df[column] = pd.to_numeric(df[column], errors="coerce")
        df[column] = df[column].where(pd.notnull(df[column]), None)


def clean_boolean_columns(df):
    bool_columns = [
        "H1B_DEPENDENT",
        "WILLFUL_VIOLATOR",
        "SUPPORT_H1B",
        "FULL_TIME_POSITION",
        "LABOR_CON_AGREE",
        "AGENT_REPRESENTING_EMPLOYER",
        "SUPPORT_H1B",
    ]
    bool_columns += [col for col in bool_columns if col in df.columns]


def clean_money_columns(df):
    money_keywords = ["RATE", "WAGE"]
    money_columns = [
        col for col in df.columns if any(keyword in col for keyword in money_keywords)
    ]
    for column in money_columns:
        # replace entries with "#" (non-meaningful) with None
        df[column] = df[column].apply(
            lambda x: None if isinstance(x, str) and "#" in x else x
        )


def map_parent_companies(df):
    parent_companies_file = "parent_employers.txt"
    with open(parent_companies_file, "r") as f:
        parent_companies = [line.strip() for line in f]
    df["PARENT_EMPLOYER_NAME"] = df["EMPLOYER_NAME"].apply(
        lambda x: map_to_parent(x, parent_companies)
    )
    return df


def clean_all_columns(df):
    clean_text_columns(df)
    clean_date_columns(df)
    clean_phone_no_columns(df)
    clean_int_columns(df)
    clean_boolean_columns(df)
    map_parent_companies(df)

    df = df.convert_dtypes()
    df = df.where(pd.notnull(df), None)

    return df


def remove_empty_columns(df):
    """remove columns where all values are either None or NaN"""
    df.dropna(axis=1, how="all", inplace=True)
    return df


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def upsert_to_supabase(df):
    unique_columns = ["CASE_NUMBER"]
    df = df.where(pd.notnull(df), None)
    data_to_insert = df.to_dict(orient="records")
    try:
        insert_response = supabase.table("lca_data").upsert(data_to_insert).execute()
        print("Data upserted to Supabase")
    except Exception as e:
        print(f"Failed to upsert data into Supabase: {e}")
        raise


def main():
    path = "/Users/jasminexli/ellis-project/datasets"

    chunks = []

    for chunk in read_excel_files(path):
        chunk = clean_all_columns(chunk)
        chunks.append(chunk)

    # concatenate all chunks into a single dataframe
    df = pd.concat(chunks, ignore_index=True)
    # perform column combining operations on the complete DF
    df = combine_ops(df, column_mappings)

    df = remove_empty_columns(df)
    upsert_to_supabase(df)


if __name__ == "__main__":
    main()
