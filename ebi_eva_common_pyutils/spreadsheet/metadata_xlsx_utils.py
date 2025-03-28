import re

from openpyxl.reader.excel import load_workbook


def metadata_xlsx_version(metadata_xlsx):
    workbook = load_workbook(metadata_xlsx)
    try:
        instructions_sheet = workbook['PLEASE READ FIRST']
        xlsx_sheet_version_value = instructions_sheet[3][0].value
        match = re.search(r'(\d+\.\d+\.\d+)', '' if xlsx_sheet_version_value is None else xlsx_sheet_version_value)
        xlsx_version = match.group(1) if match else None
    except (KeyError, IndexError):
        xlsx_version = None
    return xlsx_version