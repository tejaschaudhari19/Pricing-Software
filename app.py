import streamlit as st
import pandas as pd
import fitz
import re
import numpy as np
import io
import json
import os
from datetime import datetime
from openpyxl.styles import numbers, Border, Side
from openpyxl import load_workbook, Workbook
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl.styles import Font
import firebase_admin
from firebase_admin import credentials, firestore
from concurrent.futures import ThreadPoolExecutor
import pdfplumber

# Convert Streamlit secret section to a dict manually
firebase_dict = {
    "type": st.secrets.firebase.type,
    "project_id": st.secrets.firebase.project_id,
    "private_key_id": st.secrets.firebase.private_key_id,
    "private_key": st.secrets.firebase.private_key,
    "client_email": st.secrets.firebase.client_email,
    "client_id": st.secrets.firebase.client_id,
    "auth_uri": st.secrets.firebase.auth_uri,
    "token_uri": st.secrets.firebase.token_uri,
    "auth_provider_x509_cert_url": st.secrets.firebase.auth_provider_x509_cert_url,
    "client_x509_cert_url": st.secrets.firebase.client_x509_cert_url,
}

cred = credentials.Certificate(firebase_dict)

# === Initialize Firebase ===

if not firebase_admin._apps:
    firebase_admin.initialize_app(cred)
db = firestore.client()

# === COMMON UTILITIES ===
standard_columns = ['PORT', '20', '40STD', '40HC', 'REMARKS']

@st.cache_data
def clean_numeric_series(col):
    return pd.to_numeric(
        col.astype(str)
        .str.replace(',', '')
        .str.replace('EUR', '')
        .str.replace('USD', '')
        .str.strip(),
        errors='coerce'
    )

# def clean_numeric(value):
#     try:
#         return float(str(value).replace(',', '').replace('EUR', '').replace('USD', '').strip())
#     except:
#         return 'CASE BY CASE'
    
def clean_numeric(series):
    def convert_to_numeric(value):
        if pd.isna(value) or value == '':
            return np.nan
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            # Remove currency symbols, commas, and extra whitespace
            cleaned = re.sub(r'[^\d.]', '', value.strip())
            try:
                return float(cleaned) if cleaned else np.nan
            except ValueError:
                return np.nan
        return np.nan

    return series.apply(convert_to_numeric)

def sanitize_text(text):
    return re.sub(r'[^\x20-\x7E]+', '', text)

def normalize_text(text):
    if pd.isna(text):
        return ''
    return ' '.join(str(text).lower().strip().split())

def standardize_pol(pol):
    """Standardize POL names to a consistent format."""
    if pd.isna(pol):
        return ''
    pol = str(pol).strip().lower()
    if 'nhava' in pol and 'sheva' in pol:
        return 'Nhava Sheva'
    return pol.title()

# === PARSING FUNCTIONS ===
def parse_turkon(file, month_year):
    try:
        records = []
        with pdfplumber.open(file) as pdf:
            for page in pdf.pages:
                table = page.extract_table({
                    "vertical_strategy": "lines",
                    "horizontal_strategy": "lines",
                    "snap_tolerance": 3,
                })

                if not table:
                    continue

                for row in table:
                    if not row or len(row) < 8:
                        continue
                    if 'POD' in row and '20' in row[4]:
                        continue  # skip header

                    pol = row[0]
                    sector = row[1]
                    country = row[2]
                    pod = row[3]
                    rate_20 = row[4]
                    rate_40hc = row[5]
                    rate_40rf = row[6] if row[6] != '-' else None
                    imo = row[7]

                    if not pol or not pod or not rate_20 or not imo:
                        continue

                    # Standardize POL: if POL is 'Nsa', display as 'Nhava Sheva'
                    pol_standardized = 'Nhava Sheva' if pol.strip().lower() == 'nsa' else pol.strip()

                    records.append({
                        'POL': pol_standardized,
                        'PORT': pod.strip(),
                        "20": rate_20.strip(),
                        "40HC": rate_40hc.strip(),
                        "40'HRF": rate_40rf.strip() if rate_40rf else None,
                        "IMO SC PER TEU": imo.strip()
                    })

        df = pd.DataFrame(records)
        df['POL'] = df['POL'].apply(standardize_pol)  # Standardize POL names
        return df, []
    except Exception as e:
        print(f"Error parsing Turkon file: {str(e)}")
        return pd.DataFrame(), [f"Error parsing Turkon file: {str(e)}"]

def parse_oocl(file, month_year):
    oocl_map = {'Origins*': 'POL', 'Destinations*': 'PORT', 'Rate 20': '20', 'Rate 40': '40STD', 'Rate 40H': '40HC'}
    valid_pols = {'Nhava Sheva', 'Mundra', 'Rajula'}
    sheets = ['Nhava Sheva', 'Mundra', 'Rajula']
    data = []
    for sheet in sheets:
        try:
            df = pd.read_excel(file, sheet_name=sheet)[list(oocl_map.keys())].rename(columns=oocl_map)
            df['POL'] = df['POL'].astype(str).str.strip().replace('nan', '')
            df['POL'] = df['POL'].apply(lambda x: next((pol for pol in valid_pols if pol.lower() in x.lower()), ''))
            df = df[df['POL'] != '']
            df = df[df['PORT'].notna()]
            df['20'] = clean_numeric(df['20'])
            df['40STD'] = clean_numeric(df['40STD'])
            df['40HC'] = clean_numeric(df['40HC'])
            data.append(df)
        except ValueError:
            continue
    return pd.concat(data, ignore_index=True) if data else pd.DataFrame(), []

def parse_emirates(file, month_year):
    try:
        # Load the Excel file to get sheet names
        xl = pd.ExcelFile(file)
        sheets = xl.sheet_names
        data = []
        all_terms = []

        def parse_sheet(sheet, file):
            try:
                df = pd.read_excel(file, sheet_name=sheet, header=None)
                header_row = 0
                for idx, row in df.iterrows():
                    if any(isinstance(cell, str) and 'Destination Port' in cell for cell in row):
                        header_row = idx
                        break
                if header_row == 0 and not any('Destination Port' in str(cell) for cell in df.values.flatten()):
                    return pd.DataFrame(), []
                terms_conditions = []
                terms_active = False
                for idx, row in df.iterrows():
                    for cell in row:
                        if isinstance(cell, str) and 'Terms & Conditions' in cell:
                            terms_active = True
                            continue
                        if terms_active and isinstance(cell, str) and cell.strip():
                            cell_text = cell.strip()
                            if re.match(r'^\d+\)', cell_text) or cell_text:
                                terms_conditions.append(cell_text)
                        elif terms_active and not any(isinstance(c, str) and c.strip() for c in row):
                            terms_active = False
                            break
                df = pd.read_excel(file, sheet_name=sheet, skiprows=header_row)
                df.columns = df.columns.str.strip()
                expected_cols = {
                    'Origin Port': 'POL',
                    'Destination Port': 'PORT',
                    'Service Name': 'SERVICE NAME',
                    'Routing': 'ROUTING',
                    'Transit\n time': 'TRANSIT TIME',
                    "20'GP": '20',
                    "40'HC": '40HC',
                    'Remarks': 'REMARKS',
                    'Notes Surcharges / Subject To': 'REMARKS'
                }
                selected_cols = {}
                for excel_col, new_name in expected_cols.items():
                    if isinstance(new_name, list):
                        for possible_name in new_name:
                            if possible_name in df.columns:
                                selected_cols[possible_name] = new_name[0]
                                break
                    elif excel_col in df.columns:
                        selected_cols[excel_col] = new_name
                if not all(key in selected_cols for key in ['Origin Port', 'Destination Port', "20'GP", "40'HC"]):
                    return pd.DataFrame(), terms_conditions
                df = df[list(selected_cols.keys())].rename(columns=selected_cols)
                header_row = df.columns[0]
                df = df[df['PORT'] != header_row]
                df['20'] = clean_numeric(df['20'])
                df['40HC'] = clean_numeric(df['40HC'])
                if 'REMARKS' in df.columns and 'Notes Surcharges / Subject To' in df.columns:
                    df['REMARKS'] = df['REMARKS'].fillna('') + ' ' + df['Notes Surcharges / Subject To'].fillna('')
                    df = df.drop(columns=['Notes Surcharges / Subject To'])
                elif 'Notes Surcharges / Subject To' in df.columns:
                    df = df.rename(columns={'Notes Surcharges / Subject To': 'REMARKS'})
                text_cols = ['SERVICE NAME', 'ROUTING', 'TRANSIT TIME', 'REMARKS', 'POL']
                for col in text_cols:
                    if col in df.columns:
                        df[col] = df[col].astype(str).str.strip().replace('nan', '') if col != 'TRANSIT TIME' else df[col].astype(str).str.strip().replace('-', np.nan).replace('nan', '')
                return df[df['PORT'].notna()], terms_conditions
            except Exception as e:
                print(f"Error parsing sheet {sheet}: {str(e)}")
                return pd.DataFrame(), [f"Error parsing sheet {sheet}: {str(e)}"]

        # Iterate over sheets
        for sheet in sheets:
            df, terms_conditions = parse_sheet(sheet, file)
            if not df.empty:
                data.append(df)
            all_terms.extend(terms_conditions)

        # Combine results
        result_df = pd.concat(data, ignore_index=True) if data else pd.DataFrame()
        return result_df, all_terms

    except Exception as e:
        print(f"Error parsing Emirates file: {str(e)}")
        return pd.DataFrame(), [f"Error parsing Emirates file: {str(e)}"]

def parse_hmm(file, month_year):
    try:
        df = pd.read_excel(file, sheet_name="HMM Rate Sheet ")
        df.columns = df.iloc[0]
        df = df.drop(index=0).reset_index(drop=True)
        df.columns = df.columns.str.strip()

        if 'POL CODE' not in df.columns or 'PORTS' not in df.columns:
            print("Warning: Required columns 'POL CODE' or 'PORTS' not found in HMM Rate Sheet.")
            return pd.DataFrame(), []

        forty_std_col = None
        forty_hc_col = None
        if '40STD' in df.columns:
            forty_std_col = '40STD'
        if '40DV/HC' in df.columns:
            forty_hc_col = '40DV/HC'

        if not (forty_std_col or forty_hc_col):
            print("Warning: Neither '40STD' nor '40DV/HC' found in HMM Rate Sheet.")
            return pd.DataFrame(), []

        cols_to_fetch = ['POL CODE', 'PORTS', '20DV']
        if forty_std_col:
            cols_to_fetch.append(forty_std_col)
        if forty_hc_col:
            cols_to_fetch.append(forty_hc_col)

        parsed = df[cols_to_fetch].copy()
        rename_map = {'POL CODE': 'POL', 'PORTS': 'PORT', '20DV': '20'}
        if forty_std_col:
            rename_map[forty_std_col] = '40STD'
        if forty_hc_col:
            rename_map[forty_hc_col] = '40HC'

        parsed = parsed.rename(columns=rename_map)
        parsed = parsed.dropna(subset=['POL', 'PORT'], how='all')  # Drop rows where both POL and PORT are NaN
        parsed['POL'] = parsed['POL'].astype(str).str.strip().replace('nan', '')
        parsed['POL'] = parsed['POL'].apply(standardize_pol)  # Standardize POL names
        parsed['PORT'] = parsed['PORT'].astype(str).str.strip().replace('nan', '')
        parsed['20'] = clean_numeric(parsed['20'])

        if '40STD' in parsed.columns:
            parsed['40STD'] = clean_numeric(parsed['40STD'])
        else:
            parsed['40STD'] = np.nan

        if '40HC' in parsed.columns:
            parsed['40HC'] = clean_numeric(parsed['40HC'])
        else:
            parsed['40HC'] = np.nan

        parsed = parsed[['POL', 'PORT', '20', '40STD', '40HC']]

        def apply_currency(value):
            if pd.notna(value) and isinstance(value, (int, float)):
                return f"{value:.2f} $"
            return value

        parsed['20'] = parsed['20'].apply(apply_currency)
        if '40STD' in parsed.columns:
            parsed['40STD'] = parsed['40STD'].apply(apply_currency)
        if '40HC' in parsed.columns:
            parsed['40HC'] = parsed['40HC'].apply(apply_currency)

        # Remove rows where all rate columns are NaN or "None"
        rate_columns = [col for col in ['20', '40STD', '40HC'] if col in parsed.columns]
        if rate_columns:
            parsed = parsed.dropna(subset=rate_columns, how='all')
            parsed = parsed[~parsed[rate_columns].eq('None').all(axis=1)]

        # Remove rows where PORT is empty or "None"
        parsed = parsed[parsed['PORT'].ne('') & parsed['PORT'].ne('None')]

        if parsed['40STD'].isna().all():
            parsed = parsed.drop(columns=['40STD'])
        if parsed['40HC'].isna().all():
            parsed = parsed.drop(columns=['40HC'])

        return parsed, []
    except Exception as e:
        print(f"Error parsing HMM file: {str(e)}")
        return pd.DataFrame(), []

def parse_wan_hai(file, month_year):
    try:
        doc = fitz.open(stream=file.read(), filetype="pdf")
        text = "".join([page.get_text() for page in doc])

        # Updated regex to allow dash (-) and still capture the record
        matches = re.findall(r"([A-Z0-9 ,()'\-\w]{3,})\s+(\d{1,5})\s+(\d{1,5})\s+(\d{1,5})", text)

        # Create data list
        wan_hai_data = []
        for m in matches:
            port = m[0].strip()
            try:
                wan_hai_data.append((port, int(m[1]), int(m[2]), int(m[3])))
            except ValueError:
                # In case there is non-integer in numeric fields, skip
                pass

        # Create DataFrame
        df = pd.DataFrame(wan_hai_data, columns=['PORT', '20', '40STD', '40HC'])

        # Optionally add empty "REMARKS" column (was previously "Description")
        df['REMARKS'] = ''

        # Add POL column
        df.insert(0, 'POL', 'Nhava Sheva')
        df['POL'] = df['POL'].apply(standardize_pol)  # Standardize POL names
        return df, []
    except Exception as e:
        print(f"Error parsing Wan Hai file: {str(e)}")
        return pd.DataFrame(), []

def parse_one(file, month_year):
    data = []
    info = []
    try:
        # FAREAST and GULF
        try:
            df_fg = pd.read_excel(file, sheet_name="FAREAST and GULF")
            parsed_fg = df_fg[['FPD', "20'D", "40'D", 'HCD', 'POD COUNTRY']].copy()
            parsed_fg.columns = ['PORT', '20', '40STD', '40HC', 'POD COUNTRY']
            parsed_fg['PORT'] = parsed_fg['PORT'].astype(str).str.strip()
            parsed_fg['20'] = clean_numeric(parsed_fg['20'])
            parsed_fg['40STD'] = clean_numeric(parsed_fg['40STD'])
            parsed_fg['40HC'] = clean_numeric(parsed_fg['40HC'])
            parsed_fg['POD COUNTRY'] = parsed_fg['POD COUNTRY'].astype(str).str.strip().replace('nan', '')
            parsed_fg.insert(0, 'POL', 'Nhava Sheva')
            parsed_fg['POL'] = parsed_fg['POL'].apply(standardize_pol)  # Standardize POL names
            parsed_fg = parsed_fg.drop(columns=['POD COUNTRY'])
            data.append(parsed_fg)
        except ValueError:
            pass

        # EUR and MED
        try:
            df_eur_med_header = pd.read_excel(file, sheet_name="EUR and MED", nrows=6, header=None)
            eur_med_description = "\n".join(df_eur_med_header[0].dropna().astype(str))
            df_eur_med = pd.read_excel(file, sheet_name="EUR and MED", skiprows=6).rename(columns={
                'DEL Description': 'PORT',
                'OFT 20': '20',
                'OFT 40': '40STD',
                'OFT HC': '40HC',
                'Expiry Date': 'EXPIRY DATE'
            })
            df_eur_med['20'] = clean_numeric(df_eur_med['20'])
            df_eur_med['40STD'] = clean_numeric(df_eur_med['40STD'])
            df_eur_med['40HC'] = clean_numeric(df_eur_med['40HC'])
            df_eur_med['PORT'] = df_eur_med['PORT'].astype(str).str.strip()
            df_eur_med['EXPIRY DATE'] = pd.to_datetime(df_eur_med['EXPIRY DATE'], errors='coerce').dt.strftime('%Y-%m-%d')
            parsed_eur_med = df_eur_med[['PORT', '20', '40STD', '40HC', 'EXPIRY DATE']].copy()
            parsed_eur_med.insert(0, 'POL', 'Nhava Sheva')
            parsed_eur_med['POL'] = parsed_eur_med['POL'].apply(standardize_pol)  # Standardize POL names
            data.append(parsed_eur_med)
            info.append(eur_med_description)
        except ValueError:
            pass

        # AUS and NZ
        try:
            df_aus_nz_full = pd.read_excel(file, sheet_name="AUS and NZ", header=None)
            info_lines_aus_nz = []
            found_data = False
            for index, row in df_aus_nz_full.iterrows():
                for cell in row:
                    if isinstance(cell, str) and "PORT" in cell.upper():
                        found_data = True
                        break
                    if not found_data and isinstance(cell, str) and cell.strip():
                        info_lines_aus_nz.append(cell.strip())
                if found_data:
                    break
            remarks_start = False
            for index, row in df_aus_nz_full.iterrows():
                for cell in row:
                    if isinstance(cell, str) and "Remarks" in cell:
                        remarks_start = True
                        continue
                    if remarks_start and isinstance(cell, str) and cell.strip():
                        info_lines_aus_nz.append(cell.strip())
                    elif remarks_start and not any(isinstance(c, str) and c.strip() for c in row):
                        break
            info_lines_aus_nz = [line for line in info_lines_aus_nz if len(line.split()) > 2]
            info_lines_aus_nz = list(dict.fromkeys(info_lines_aus_nz))
            df_aus_nz = pd.read_excel(file, sheet_name="AUS and NZ", skiprows=3)
            df_aus_nz.columns = df_aus_nz.columns.map(str).str.strip().str.replace("’", "'").str.replace("‘", "'")
            target_columns = ['PORT', "20'", "40'", "40'HC"]
            col_map = {'PORT': 'PORT', "20'": '20', "40'": '40STD', "40'HC": '40HC'}
            available_targets = [col for col in target_columns if col in df_aus_nz.columns]
            if not available_targets:
                col_map = {}
                for col in df_aus_nz.columns:
                    col_lower = col.lower()
                    if 'pol' in col_lower or 'dest' in col_lower:
                        col_map[col] = 'PORT'
                    elif '20' in col_lower:
                        col_map[col] = '20'
                    elif '40' in col_lower:
                        col_map[col] = '40STD'
                    elif '40' in col_lower and 'hc' in col_lower:
                        col_map[col] = '40HC'
                available_targets = list(col_map.keys())
                if not available_targets:
                    col_map = {df_aus_nz.columns[1]: 'PORT', df_aus_nz.columns[4]: '20', df_aus_nz.columns[5]: '40STD', df_aus_nz.columns[6]: '40HC'}
                    available_targets = list(col_map.keys())
            parsed_aus_nz = df_aus_nz[available_targets].rename(columns=col_map)
            parsed_aus_nz = parsed_aus_nz[parsed_aus_nz['PORT'].str.lower() != 'port']
            parsed_aus_nz = parsed_aus_nz.dropna(subset=['PORT', '20', '40STD', '40HC'])
            parsed_aus_nz['PORT'] = parsed_aus_nz['PORT'].astype(str).str.strip()
            parsed_aus_nz['20'] = clean_numeric(parsed_aus_nz['20'])
            parsed_aus_nz['40STD'] = clean_numeric(parsed_aus_nz['40STD'])
            parsed_aus_nz['40HC'] = clean_numeric(parsed_aus_nz['40HC'])
            parsed_aus_nz.insert(0, 'POL', 'Nhava Sheva')
            parsed_aus_nz['POL'] = parsed_aus_nz['POL'].apply(standardize_pol)  # Standardize POL names
            data.append(parsed_aus_nz)
            info.extend(info_lines_aus_nz)
        except ValueError:
            pass

        # AFRICA
        try:
            df_africa_full = pd.read_excel(file, sheet_name="AFRICA", header=None)
            info_lines_africa = []
            data_section_started = False
            for index, row in df_africa_full.iterrows():
                for cell in row:
                    if isinstance(cell, str) and "PORT" in cell.upper() and not data_section_started:
                        data_section_started = True
                        continue
                    if isinstance(cell, str) and cell.strip():
                        info_lines_africa.append(cell.strip())
                    if data_section_started and not any(isinstance(c, str) and c.strip() for c in row) and index > 5:
                        break
            info_lines_africa = [line for line in info_lines_africa if len(line.split()) > 2]
            info_lines_africa = list(dict.fromkeys(info_lines_africa))
            df_africa = pd.read_excel(file, sheet_name="AFRICA", skiprows=3)
            df_africa.columns = df_africa.columns.map(str).str.strip().str.replace("’", "'").str.replace("‘", "'")
            target_columns = ['PORT', "20'", "40'", "40'HC", 'Remarks']
            col_map = {'PORT': 'PORT', "20'": '20', "40'": '40STD', "40'HC": '40HC', 'Remarks': 'REMARKS'}
            available_targets = [col for col in target_columns if col in df_africa.columns]
            if not available_targets:
                col_map = {}
                for col in df_africa.columns:
                    col_lower = col.lower()
                    if 'port' in col_lower or 'dest' in col_lower:
                        col_map[col] = 'PORT'
                    elif '20' in col_lower:
                        col_map[col] = '20'
                    elif '40' in col_lower and 'hc' not in col_lower:
                        col_map[col] = '40STD'
                    elif '40' in col_lower and 'hc' in col_lower:
                        col_map[col] = '40HC'
                    elif 'remark' in col_lower or 'note' in col_lower:
                        col_map[col] = 'REMARKS'
                available_targets = list(col_map.keys())
            parsed_africa = df_africa[available_targets].rename(columns=col_map)
            parsed_africa = parsed_africa[parsed_africa['PORT'].str.lower() != 'port']
            desired_columns = [col for col in ['PORT', '20', '40STD', '40HC', 'REMARKS'] if col in parsed_africa.columns]
            parsed_africa = parsed_africa.dropna(subset=desired_columns)
            parsed_africa['PORT'] = parsed_africa['PORT'].astype(str).str.strip()
            parsed_africa['20'] = clean_numeric(parsed_africa['20'])
            parsed_africa['40STD'] = clean_numeric(parsed_africa['40STD'])
            parsed_africa['40HC'] = clean_numeric(parsed_africa['40HC'])
            if 'REMARKS' in parsed_africa.columns:
                parsed_africa['REMARKS'] = parsed_africa['REMARKS'].astype(str).str.strip().replace('nan', '')
            parsed_africa.insert(0, 'POL', 'Nhava Sheva')
            parsed_africa['POL'] = parsed_africa['POL'].apply(standardize_pol)  # Standardize POL names
            data.append(parsed_africa)
            info.extend(info_lines_africa)
        except Exception as e:
            print(f"Error parsing AFRICA sheet: {str(e)}")
    except Exception as e:
        print(f"Error in parse_one: {str(e)}")
    return pd.concat([df for df in data if not df.empty], ignore_index=True) if data else pd.DataFrame(), info

def parse_msc(file, month_year):
    try:
        df = pd.read_excel(file, header=None)
        data_start = 0
        for idx, row in df.iterrows():
            if isinstance(row.iloc[0], str) and 'SECTOR' in row.iloc[0].upper():
                data_start = idx + 1
                break
        if data_start == 0:
            return pd.DataFrame(), []
        df_data = df.iloc[data_start:].reset_index(drop=True)
        parsed = pd.DataFrame({
            'PORT': df_data.iloc[:, 0].dropna().astype(str).str.strip(),
            '20': df_data.iloc[:, 1],
            '40STD': df_data.iloc[:, 2],
            'REMARKS': df_data.iloc[:, 3].fillna('').astype(str).str.strip()  # Renamed from Description
        })
        parsed = parsed[parsed['PORT'].str.strip() != ''].dropna(subset=['PORT'])
        desired_columns = ['PORT', '20', '40STD', 'REMARKS']
        parsed = parsed.dropna(subset=desired_columns)

        def extract_value_with_currency(value):
            if isinstance(value, str):
                value = value.strip()
                if 'EUR' in value.upper():
                    match = re.search(r'(\d+[.,]?\d*)', value)
                    if match:
                        return f"{float(match.group(1).replace(',', ''))} €"
                else:
                    match = re.search(r'(\d+[.,]?\d*)', value)
                    if match:
                        return f"{float(match.group(1).replace(',', ''))} $"
            elif isinstance(value, (int, float)):
                return f"{float(value)} $"
            return np.nan

        parsed['20'] = parsed['20'].apply(extract_value_with_currency)
        parsed['40STD'] = parsed['40STD'].apply(extract_value_with_currency)
        parsed.insert(0, 'POL', 'Nhava Sheva')
        parsed['POL'] = parsed['POL'].apply(standardize_pol)  # Standardize POL names
        return parsed, []
    except Exception as e:
        print(f"Error in parse_msc: {str(e)}")
        return pd.DataFrame(), []

def parse_pil(file, month_year):
    try:
        df_pil_raw = pd.read_excel(file, sheet_name=0, header=None)
        info = []
        notes_section = False
        for idx, row in df_pil_raw.iterrows():
            for cell in row:
                cell_str = str(cell).strip()
                if any(keyword in cell_str.lower() for keyword in ['subject to', 'sub to']) and 'valid till' not in cell_str.lower():
                    notes_section = True
                if notes_section and cell_str and cell_str != 'nan':
                    info.append(cell_str)
        info = list(dict.fromkeys([i for i in info if any(keyword in i.lower() for keyword in ['subject to', 'sub to'])]))[:10]

        header_row = None
        expected_cols = ['POL', 'POD', "20'FT ($)", "40'FT ($)"]
        for idx, row in df_pil_raw.iterrows():
            row_values = row.astype(str).str.lower().str.strip()
            if all(any(col.lower() in val for val in row_values) for col in expected_cols) and not any('offered rates' in val for val in row_values):
                header_row = idx
                break

        if header_row is None:
            return pd.DataFrame(), ["Could not find header row with expected columns: 'POL', 'POD', '20'FT ($)', '40'FT ($)'. Please check the file structure."]

        df_pil = pd.read_excel(file, sheet_name=0, skiprows=header_row)
        pil_map = {
            'POL': 'POL',
            'POD': 'PORT',
            "20'FT ($)": '20',
            "40'FT ($)": '40STD',
            'ROUTINE': 'ROUTING',
            'APPX. T/TIME': 'TRANSIT TIME',
            'Validity': 'VALIDITY',
            'Remarks ': 'REMARKS'
        }

        required_cols = ['POL', 'POD', "20'FT ($)", "40'FT ($)"]
        missing_cols = [col for col in required_cols if col not in df_pil.columns]
        if missing_cols:
            return pd.DataFrame(), [f"Missing required columns: {missing_cols}. Available columns: {df_pil.columns.tolist()}"]

        base_cols = [col for col in required_cols if col in df_pil.columns]
        parsed_pil = df_pil[base_cols].rename(columns={col: pil_map[col] for col in base_cols})

        optional_cols = ['ROUTINE', 'APPX. T/TIME', 'Validity', 'Remarks ']
        for col in optional_cols:
            if col in df_pil.columns:
                parsed_pil[pil_map[col]] = df_pil[col].astype(str).str.strip().replace('nan', '')

        parsed_pil['20'] = clean_numeric(parsed_pil['20'])
        parsed_pil['40STD'] = clean_numeric(parsed_pil['40STD'])

        if '40HC' in parsed_pil.columns:
            parsed_pil = parsed_pil.drop(columns=['40HC'])

        parsed_pil['ROUTING'] = parsed_pil.get('ROUTING', '').astype(str).str.strip().replace('nan', '')
        parsed_pil['TRANSIT TIME'] = parsed_pil.get('TRANSIT TIME', '').astype(str).str.strip().replace('nan', '')

        parsed_pil['PORT'] = parsed_pil['PORT'].astype(str).str.strip().replace('nan', '')
        parsed_pil['POL'] = parsed_pil['POL'].astype(str).str.strip().replace('nan', '')
        parsed_pil['POL'] = parsed_pil['POL'].apply(standardize_pol)  # Standardize POL names

        parsed_pil = parsed_pil[~parsed_pil['PORT'].str.upper().str.contains('PORTS', na=False)]
        parsed_pil = parsed_pil.dropna(subset=['PORT', 'POL'])
        parsed_pil = parsed_pil[(parsed_pil['PORT'].str.strip() != '') & (parsed_pil['POL'].str.strip() != '')]

        parsed_pil = parsed_pil[~((parsed_pil['20'].isna() | (parsed_pil['20'] == 0)) & 
                                  (parsed_pil['40STD'].isna() | (parsed_pil['40STD'] == 0)))]

        return parsed_pil, info
    except Exception as e:
        print(f"Error parsing PIL MRG file: {str(e)}")
        return pd.DataFrame(), [f"Error parsing PIL MRG file: {str(e)}"]

def parse_arkas(file, month_year):
    try:
        df_arkas_raw = pd.read_excel(file, sheet_name=0, header=None)
        header_row = None
        for idx, row in df_arkas_raw.iterrows():
            row_values = row.astype(str).str.lower().str.strip()
            if any('pol' in val for val in row_values) and any('pod' in val for val in row_values):
                header_row = idx
                break

        if header_row is None:
            return pd.DataFrame(), ["Could not find header row with 'POL' and 'POD'."]

        df_arkas = pd.read_excel(file, sheet_name=0, skiprows=header_row)
        arkas_map = {
            'POL': 'POL',
            'POD': 'PORT',
            "t2 20'": '20',
            "t2 40'": '40STD'
        }

        required_cols = ['POL', 'POD', "t2 20'", "t2 40'"]
        missing_cols = [col for col in required_cols if col not in df_arkas.columns]
        if missing_cols:
            return pd.DataFrame(), [f"Missing required columns: {missing_cols}. Available columns: {df_arkas.columns.tolist()}"]

        base_cols = [col for col in required_cols if col in df_arkas.columns]
        parsed_arkas = df_arkas[base_cols].rename(columns={col: arkas_map[col] for col in base_cols})

        parsed_arkas['20'] = clean_numeric(parsed_arkas['20'])
        parsed_arkas['40STD'] = clean_numeric(parsed_arkas['40STD'])
        parsed_arkas['40HC'] = np.nan

        parsed_arkas['PORT'] = parsed_arkas['PORT'].astype(str).str.strip().replace('nan', '')
        parsed_arkas['POL'] = parsed_arkas['POL'].astype(str).str.strip().replace('nan', '')
        parsed_arkas['POL'] = parsed_arkas['POL'].replace({'INMUN': 'Mundra', 'INNSA': 'Nhava Sheva'})
        parsed_arkas['POL'] = parsed_arkas['POL'].apply(standardize_pol)  # Standardize POL names

        unwanted_keywords = ['PORTS', 'TOTAL', 'REMARK', 'CHARGE', 'OTHERS']
        parsed_arkas = parsed_arkas[~parsed_arkas['PORT'].str.upper().str.contains('|'.join(unwanted_keywords), na=False)]

        parsed_arkas = parsed_arkas.dropna(subset=['PORT'])
        parsed_arkas = parsed_arkas[parsed_arkas['PORT'].str.strip() != '']

        parsed_arkas = parsed_arkas[~((parsed_arkas['20'].isna() | (parsed_arkas['20'] == 0)) &
                                      (parsed_arkas['40STD'].isna() | (parsed_arkas['40STD'] == 0)))]

        parsed_arkas = parsed_arkas.dropna(subset=['POL', 'PORT'])
        parsed_arkas = parsed_arkas[(parsed_arkas['POL'].str.strip() != '') & (parsed_arkas['PORT'].str.strip() != '')]

        return parsed_arkas, []
    except Exception as e:
        print(f"Error parsing ARKAS MRG file: {str(e)}")
        return pd.DataFrame(), [f"Error parsing ARKAS MRG file: {str(e)}"]

def parse_interasia(file, month_year):
    try:
        # === STEP 1: Read Excel with pandas ===
        df_raw = pd.read_excel(file, header=6)

        # === STEP 2: Clean and map relevant columns ===
        column_map = {
            'POD': 'PORT',
            "FRT FOR 20'SD": '20',
            "FRT FOR 40'HC": '40HC',
            "FRT FOR 20'SD HAZ": '20 Haz',
            "FRT FOR 40'HC HAZ": '40 Haz',
            'ROUTING': 'ROUTING',
            'TRANSIT': 'TRANSIT TIME',
            'SERVICE': 'SERVICE',
        }
        df = df_raw[list(column_map.keys())].rename(columns=column_map)

        # Add static POL column
        df.insert(0, 'POL', 'Nhava Sheva')
        df['POL'] = df['POL'].apply(standardize_pol)  # Standardize POL names

        # === STEP 3: Dynamically extract REMARKS (was Additional Information) ===
        # Load workbook to read merged cells
        wb = load_workbook(file, data_only=True)
        ws = wb.active

        # Find where SERVICE is
        service_col_index = df_raw.columns.get_loc('SERVICE')
        remarks_col_index = service_col_index + 1  # next column after SERVICE

        # Add REMARKS column
        df['REMARKS'] = ''

        # Build a mapping: Excel Row Number → REMARKS
        row_to_info = {}

        for merged_range in ws.merged_cells.ranges:
            min_col, min_row, max_col, max_row = merged_range.bounds
            if min_col > service_col_index + 1:  # merged cell to the right of SERVICE
                merged_value = ws.cell(row=min_row, column=min_col).value
                if merged_value:
                    for r in range(min_row, max_row + 1):
                        row_to_info[r] = str(merged_value).strip()

        # Fill REMARKS based on Excel row numbers
        excel_start_row = 8  # because header is row 7, data starts at row 8
        for i in range(len(df)):
            excel_row_num = i + excel_start_row
            if excel_row_num in row_to_info:
                df.at[i, 'REMARKS'] = row_to_info[excel_row_num]
            else:
                df.at[i, 'REMARKS'] = ''  # empty if no info

        # === STEP 4: Clean Freight Columns ===
        def preserve_dollar(val):
            if pd.isna(val):
                return ''
            val_str = str(val).strip()
            if any(c.isdigit() for c in val_str) or '$' in val_str:
                cleaned = val_str.replace('USD', '').replace(' ', '')
                return f"${cleaned}" if '$' not in cleaned and any(c.isdigit() for c in cleaned) else cleaned
            return val_str

        df['20'] = df['20'].apply(preserve_dollar)
        df['40HC'] = df['40HC'].apply(preserve_dollar)
        df['20 Haz'] = df['20 Haz'].apply(preserve_dollar)
        df['40 Haz'] = df['40 Haz'].apply(preserve_dollar)

        # Merge comments across 20/40 and 20 Haz/40 Haz into REMARKS
        for index, row in df.iterrows():
            # Merge 20 and 40HC
            comment_20 = row['20'] if not any(c.isdigit() for c in str(row['20'])) else ''
            comment_40 = row['40HC'] if not any(c.isdigit() for c in str(row['40HC'])) else ''
            if comment_20 or comment_40:
                df.at[index, '20'] = comment_20 or comment_40
                df.at[index, '40HC'] = comment_20 or comment_40
                df.at[index, 'REMARKS'] = (df.at[index, 'REMARKS'] + ' ' + (comment_20 or comment_40)).strip()

            # Merge 20 Haz and 40 Haz
            comment_20_haz = row['20 Haz'] if not any(c.isdigit() for c in str(row['20 Haz'])) else ''
            comment_40_haz = row['40 Haz'] if not any(c.isdigit() for c in str(row['40 Haz'])) else ''
            if comment_20_haz or comment_40_haz:
                df.at[index, '20 Haz'] = comment_20_haz or comment_40_haz
                df.at[index, '40 Haz'] = comment_20_haz or comment_40_haz
                df.at[index, 'REMARKS'] = (df.at[index, 'REMARKS'] + ' ' + (comment_20_haz or comment_40_haz)).strip()

        df['SERVICE'] = df['SERVICE'].fillna('')

        # === STEP 5: Filter only rows with non-empty PORT ===
        df = df[
            (df['PORT'].notna() & df['PORT'].astype(str).str.strip().ne(''))
        ]

        return df, []
    except Exception as e:
        print(f"Error parsing Interasia file: {str(e)}")
        return pd.DataFrame(), []

def parse_cosco_gulf(file, month_year):
    try:
        with pdfplumber.open(file) as pdf:
            data = []
            pol_data = {pol: [] for pol in ["NHAVASHEVA", "MUNDRA", "HAZIRA"]}  # Store data per POL
            info_lines = []

            # Iterate through pages
            for page_num, page in enumerate(pdf.pages, 1):
                text = page.extract_text()
                if not text:
                    continue

                # Extract informational text
                info_match = re.search(r"(Bookings should not be placed without filing a rate.*?(?=\Z))", text, re.DOTALL)
                if info_match:
                    info_text = info_match.group(1).strip()
                    info_lines.extend([line.strip() for line in info_text.split('\n') if line.strip() and len(line.split()) > 2])

                # Split text into lines and process
                lines = [line.strip() for line in text.split('\n') if line.strip()]

                for line_idx, line in enumerate(lines):
                    if "PORT NAME" in line.upper():
                        continue
                    elif any(pol in line.upper() for pol in ["NHAVASHEVA", "MUNDRA", "HAZIRA"]):
                        continue
                    elif re.match(r'^[A-Za-z\s\-\'(),;]+\s+\d+\s+\d+\s+\d+\s+\d+\s+\d+\s+\d+$', line):
                        # Extract full port name with all punctuation and rates
                        match = re.match(r'^([A-Za-z\s\-\'(),;]+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)$', line)
                        if match:
                            port_name = match.group(1).strip()  # Full port name with punctuation
                            rate_parts = [int(x) for x in match.groups()[1:]]  # Six rate values
                            if len(rate_parts) == 6:
                                for i, pol in enumerate(["NHAVASHEVA", "MUNDRA", "HAZIRA"]):
                                    rate_20 = rate_parts[i * 2]
                                    rate_40 = rate_parts[i * 2 + 1]
                                    if rate_20 > 0 and rate_40 > 0:
                                        pol_data[pol].append({
                                            'PORT': port_name,
                                            '20': rate_20,
                                            '40STD': rate_40
                                        })

            # Combine data in POL order
            for pol in ["NHAVASHEVA", "MUNDRA", "HAZIRA"]:
                for entry in pol_data[pol]:
                    data.append({
                        'POL': pol,
                        'PORT': entry['PORT'],
                        '20': entry['20'],
                        '40STD': entry['40STD']
                    })

            # Create DataFrame
            parsed_cosco_gulf = pd.DataFrame(data) if data else pd.DataFrame(columns=['POL', 'PORT', '20', '40STD'])
            
            # Standardize column names and ensure POL is correctly formatted
            parsed_cosco_gulf['40HC'] = np.nan  # Add 40HC column for consistency
            parsed_cosco_gulf['POL'] = parsed_cosco_gulf['POL'].apply(standardize_pol)  # Standardize POL names
            parsed_cosco_gulf['PORT'] = parsed_cosco_gulf['PORT'].astype(str).str.strip()
            parsed_cosco_gulf['20'] = pd.to_numeric(parsed_cosco_gulf['20'], errors='coerce')
            parsed_cosco_gulf['40STD'] = pd.to_numeric(parsed_cosco_gulf['40STD'], errors='coerce')

            # Remove empty or invalid rows
            parsed_cosco_gulf = parsed_cosco_gulf.dropna(subset=['PORT', '20', '40STD'])
            parsed_cosco_gulf = parsed_cosco_gulf[parsed_cosco_gulf['PORT'].str.strip() != '']

            return parsed_cosco_gulf, info_lines

    except Exception as e:
        print(f"Error parsing Cosco-Gulf file: {str(e)}")
        return pd.DataFrame(columns=['POL', 'PORT', '20', '40STD', '40HC']), [f"Error parsing Cosco-Gulf file: {str(e)}"]

def parse_cosco_wcsa_cb(file, month_year):
    try:
        with pdfplumber.open(file) as pdf:
            data = []
            wcsa_remarks = []  # Isolated remarks for WCSA & CB

            # Iterate through pages
            for page_num, page in enumerate(pdf.pages, 1):
                # Extract full text for remarks and data
                text = page.extract_text()
                lines = [line.strip() for line in text.split('\n') if line.strip()]
                table_started = False
                potential_data_lines = []

                # First pass: Collect potential data and remarks
                for line in lines:
                    # Detect table start with flexible header match
                    if not table_started and any(col in line.upper() for col in ["20'", "40'", "ROUTING", "T.TIME"]):
                        table_started = True
                    elif not table_started:
                        wcsa_remarks.append(line)
                    elif table_started:
                        potential_data_lines.append(line)

                # Second pass: Process data lines, treat non-matches as remarks
                for line in potential_data_lines:
                    # Match data rows with flexible spacing
                    match = re.match(
                        r'^([A-Za-z\s,\/&-]+?)\s*(\d+)\s*(\d+)\s*([A-Za-z]+)\s*(\d+)\s*[-–]\s*(\d+)\s*days$',
                        line,
                        re.IGNORECASE
                    )
                    if match:
                        port = match.group(1).strip()
                        rate_20 = int(match.group(2))
                        rate_40 = int(match.group(3))
                        routing = match.group(4).strip()
                        t_time = f"{match.group(5)} - {match.group(6)} days"
                        if rate_20 > 0 and rate_40 > 0:
                            data.append({
                                'POL': 'Nhava Sheva',
                                'PORT': port,
                                '20': rate_20,
                                '40STD': rate_40,
                                'ROUTING': routing,
                                'TRANSIT TIME': t_time  # Renamed from T.Time (Approx)
                            })
                            data.append({
                                'POL': 'Mundra',
                                'PORT': port,
                                '20': rate_20,
                                '40STD': rate_40,
                                'ROUTING': routing,
                                'TRANSIT TIME': t_time  # Renamed from T.Time (Approx)
                            })
                    else:
                        wcsa_remarks.append(line)

            # Create DataFrame with Nhava Sheva first, then Mundra
            nhava_data = [d for d in data if d['POL'] == 'Nhava Sheva']
            mundra_data = [d for d in data if d['POL'] == 'Mundra']
            parsed_wcsa_cb = pd.DataFrame(nhava_data + mundra_data) if data else pd.DataFrame(columns=['POL', 'PORT', '20', '40STD', 'ROUTING', 'TRANSIT TIME'])

            # Standardize column names
            parsed_wcsa_cb['40HC'] = np.nan  # Add 40HC column for consistency
            parsed_wcsa_cb['POL'] = parsed_wcsa_cb['POL'].apply(standardize_pol)  # Standardize POL names

            # Remove empty or invalid rows
            parsed_wcsa_cb = parsed_wcsa_cb.dropna(subset=['PORT', '20', '40STD'])
            parsed_wcsa_cb = parsed_wcsa_cb[parsed_wcsa_cb['PORT'].str.strip() != '']

            return parsed_wcsa_cb, wcsa_remarks

    except Exception as e:
        print(f"Error parsing Cosco-WCSA & CB file: {str(e)}")
        return pd.DataFrame(columns=['POL', 'PORT', '20', '40STD', '40HC', 'ROUTING', 'TRANSIT TIME']), [f"Error parsing Cosco-WCSA & CB file: {str(e)}"]

def parse_cosco_africa(file, month_year):
    try:
        records = []
        raw_lines = []
        current_section = ""
        pipavav_allowed = False
        last_matched_line_index = -1
        port_sequence = []
        port_seen = set()

        with pdfplumber.open(file) as pdf:
            for page in pdf.pages:
                text = page.extract_text()
                if not text:
                    continue
                raw_lines.extend([line.strip() for line in text.split('\n') if line.strip()])

        for idx, line in enumerate(raw_lines):
            # Detect section headers
            if re.search(r'WEST AFRICA.*SOUTH WEST AFRICA', line, re.I):
                current_section = line.strip()
                pipavav_allowed = True
                continue
            elif re.search(r'\bEAST AFRICA\b', line, re.I):
                current_section = line.strip()
                pipavav_allowed = False
                continue
            elif re.search(r'\bSOUTH AFRICA\b', line, re.I):
                current_section = line.strip()
                pipavav_allowed = False
                continue

            # Match port + rates
            match = re.match(r'^(.+?)\s+(\d{3,5})\s+(\d{3,5})$', line)
            if match:
                port = match.group(1).strip()
                rate_20 = int(match.group(2))
                rate_40 = int(match.group(3))

                if port not in port_seen:
                    port_sequence.append(port)
                    port_seen.add(port)
                last_matched_line_index = idx

                for pol in ['Nhava Sheva', 'Mundra']:
                    records.append({
                        'POL': pol,
                        'PORT': port,
                        '20': rate_20,
                        '40STD': rate_40,
                        'REMARKS': current_section
                    })
                if pipavav_allowed:
                    records.append({
                        'POL': 'Pipavav',
                        'PORT': port,
                        '20': rate_20,
                        '40STD': rate_40,
                        'REMARKS': current_section
                    })

        # Collect all remaining lines as post-table remarks
        full_post_table_remarks = raw_lines[last_matched_line_index + 1:]

        # Create DataFrame
        parsed_cosco_africa = pd.DataFrame(records)

        # Standardize column names and ensure POL is correctly formatted
        parsed_cosco_africa['40HC'] = np.nan  # Add 40HC column for consistency
        parsed_cosco_africa['POL'] = parsed_cosco_africa['POL'].apply(standardize_pol)  # Standardize POL names

        # Remove empty or invalid rows
        parsed_cosco_africa = parsed_cosco_africa.dropna(subset=['PORT', '20', '40STD'])
        parsed_cosco_africa = parsed_cosco_africa[parsed_cosco_africa['PORT'].str.strip() != '']

        return parsed_cosco_africa, full_post_table_remarks

    except Exception as e:
        print(f"Error parsing Cosco-Africa file: {str(e)}")
        return pd.DataFrame(columns=['POL', 'PORT', '20', '40STD', '40HC', 'REMARKS']), [f"Error parsing Cosco-Africa file: {str(e)}"]

def parse_cosco_fareast(file, month_year):
    try:
        pols = ["Nhava Sheva", "Mundra", "Pipavav"]
        all_rows = []
        additional_info = []

        # Flexible regex to handle variations in spacing and port names
        pattern = re.compile(r"([A-Za-z\s\-/().,']+?)\s+(\d{2,6}(?:\.\d+)?)\s+(\d{2,6}(?:\.\d+)?)\s+(\d{2,6}(?:\.\d+)?)")

        with pdfplumber.open(file) as pdf:
            for page in pdf.pages:
                text = page.extract_text()
                if not text:
                    continue
                print(f"Raw text from page {page.page_number}:\n{text}\n")  # Debug: Log raw text
                lines = [line.strip() for line in text.split('\n') if line.strip()]
                
                for line in lines:
                    # Skip lines that are clearly not data
                    if line.startswith("*") or any(keyword in line.lower() for keyword in [
                        "rates are", "currently", "note", "booking", "surcharge",
                        "valid", "no service", "not accepting", "weight limitation"]):
                        additional_info.append(line)
                        print(f"Filtered out as info: {line}")  # Debug
                        continue

                    # Try to match the line
                    matches = pattern.findall(line)
                    print(f"Line: {line}, Matches: {matches}")  # Debug: Log matches
                    for idx, match in enumerate(matches):
                        port, r20, r40std, r40hc = match
                        port = port.strip()
                        if not port or len(port) < 3:  # Skip invalid ports
                            continue
                        pol = pols[idx % len(pols)]  # Cycle through POLs
                        try:
                            r20 = float(r20)
                            r40std = float(r40std)
                            r40hc = float(r40hc)
                            if r20 <= 0 and r40std <= 0 and r40hc <= 0:  # Skip invalid rates
                                continue
                            all_rows.append({
                                'POL': pol,
                                'PORT': port,
                                '20': r20,
                                '40STD': r40std,
                                '40HC': r40hc,
                                'REMARKS': ''
                            })
                            print(f"Added record: POL={pol}, PORT={port}, 20={r20}, 40STD={r40std}, 40HC={r40hc}")  # Debug
                        except ValueError as e:
                            print(f"Error converting rates for line {line}: {str(e)}")  # Debug
                            continue

        df = pd.DataFrame(all_rows)
        print(f"Initial DataFrame:\n{df}\n")  # Debug: Log DataFrame

        if not df.empty:
            # Clean and standardize data
            df['POL'] = df['POL'].apply(standardize_pol)
            df['PORT'] = df['PORT'].astype(str).str.strip()
            df = df[df['PORT'].notna() & (df['PORT'].str.strip() != '')]
            df['REMARKS'] = df['REMARKS'].astype(str).str.strip().replace('', np.nan)
            
            # Format numeric columns
            for col in ['20', '40STD', '40HC']:
                df[col] = pd.to_numeric(df[col], errors='coerce')
                df[col] = df[col].apply(lambda x: f"{x:.2f}" if pd.notna(x) else np.nan)
            
            # Sort by POL and PORT
            df['POL_ORDER'] = df['POL'].map({'Nhava Sheva': 1, 'Mundra': 2, 'Pipavav': 3})
            df = df.sort_values(by=['POL_ORDER', 'PORT']).drop(columns=['POL_ORDER'])
            
            # Drop rows with all rates missing
            df = df.dropna(subset=['20', '40STD', '40HC'], how='all')
            print(f"Final DataFrame:\n{df}\n")  # Debug: Log final DataFrame

        additional_info = list(dict.fromkeys(additional_info))
        print(f"Additional Info: {additional_info}")  # Debug
        return df, additional_info

    except Exception as e:
        print(f"Error parsing Cosco-Fareast file: {str(e)}")
        return pd.DataFrame(columns=['POL', 'PORT', '20', '40STD', '40HC', 'REMARKS']), [f"Error parsing Cosco-Fareast file: {str(e)}"]    
import pandas as pd
import numpy as np
from openpyxl import load_workbook
import traceback
import re

def parse_zim(file, month_year):
    try:
        xl = pd.ExcelFile(file)
        sheets = xl.sheet_names
        data_frames = []
        all_remarks = []

        def clean_numeric_series(col):
            def clean_value(val):
                if pd.isna(val) or val is None or str(val).strip() == '':
                    return np.nan
                if isinstance(val, (int, float)):
                    return float(val)
                val_str = str(val).strip()
                val_clean = val_str.replace(',', '').replace('EUR', '').replace('USD', '').replace('$', '').replace('€', '').replace(' ', '').strip()
                if not val_clean.replace('.', '').replace('-', '').isdigit():
                    print(f"Invalid value in clean_numeric_series: '{val_str}'")
                    return np.nan
                try:
                    return float(val_clean)
                except (ValueError, TypeError) as e:
                    print(f"Error converting value '{val_str}' to float: {str(e)}")
                    return np.nan
            return col.apply(clean_value)

        def standardize_pol(pol):
            if pd.isna(pol):
                return ''
            pol = str(pol).strip().upper()
            mapping = {
                'INNHV': 'Nhava Sheva',
                'NHAVA SHEVA': 'Nhava Sheva',
                'NHV/RQ': 'Nhava Sheva',
                'NHAVA': 'Nhava Sheva',
                'NSA': 'Nhava Sheva',
                'NS': 'Nhava Sheva',
                'MUNDRA': 'Mundra',
                'PIPAVAV': 'Pipavav',
                'TUTICORIN': 'Tut',
                'TUT': 'Tut',
                'KATTUPALLI': 'Ktp',
                'KOLKATA': 'Ccu',
                'CCU': 'Ccu',
                'WRG': 'Wrg',
                'INMUN': 'Mundra',
                'INNSA': 'Nhava Sheva',
                'HAZIRA': 'Hazira',
                'HZ': 'Hazira',
                'INHZA': 'Hazira'
            }
            return mapping.get(pol, pol.title())

        def process_turkey_med_sheet(sheet_name):
            remarks = []
            try:
                wb = load_workbook(file, data_only=True)
                ws_data = wb[sheet_name]
                records = []
                header_row = None

                for row_idx, row in enumerate(ws_data.iter_rows(min_row=1, max_row=100), start=1):
                    row_vals = [str(cell.value).strip().upper() for cell in row if cell.value not in (None, '')]
                    row_str = ' '.join(row_vals)
                    if any(x in row_str for x in ['NHAVA', 'NS', 'NSA', 'NHV', 'INNHV', 'HAZIRA', 'HZ', 'INHZA', 'POD', 'PORT', 'DEST', 'DESTINATION', 'DISCHARGE', '20', '40', 'HC']):
                        header_row = row
                        header_row_idx = row_idx
                        break
                if not header_row:
                    print(f"No header row found in sheet '{sheet_name}' after searching 100 rows")
                    for row_idx, row in enumerate(ws_data.iter_rows(min_row=1, max_row=50), start=1):
                        print(f"Row {row_idx}: {[str(cell.value) for cell in row]}")
                    return fallback_parse_turkey_med(file, sheet_name, remarks)

                print(f"Found header row {header_row_idx} in '{sheet_name}': {[str(cell.value) for cell in header_row]}")
                col_map = {}
                for col_idx, cell in enumerate(header_row):
                    val = str(cell.value).strip().upper() if cell.value else ''
                    if any(n in val for n in ['NHAVA', 'NS', 'NSA', 'NHV', 'INNHV']):
                        if '20' in val:
                            col_map['nhv_20'] = col_idx
                        elif any(h in val for h in ['40', 'HC']):
                            col_map['nhv_40'] = col_idx
                    elif any(h in val for h in ['HAZIRA', 'HZ', 'INHZA']):
                        if '20' in val:
                            col_map['hzr_20'] = col_idx
                        elif any(h in val for h in ['40', 'HC']):
                            col_map['hzr_40'] = col_idx
                    elif any(p in val for p in ['POD', 'PORT', 'DEST', 'DESTINATION', 'DISCHARGE', 'DISCHARGE PORT', 'DEST. PORT']):
                        col_map['pod'] = col_idx
                print(f"Column mapping for '{sheet_name}': {col_map}")

                if 'pod' not in col_map:
                    print(f"No POD column identified in sheet '{sheet_name}'")
                    return fallback_parse_turkey_med(file, sheet_name, remarks)

                for row_idx, row in enumerate(ws_data.iter_rows(min_row=header_row_idx + 1), start=header_row_idx + 1):
                    pod = str(row[col_map['pod']].value).strip() if col_map['pod'] < len(row) and row[col_map['pod']].value not in (None, '') else ''
                    if not pod:
                        print(f"Skipping row {row_idx} in sheet '{sheet_name}': Empty POD")
                        continue
                    if len(pod.split()) > 3:
                        remarks.append(pod)
                        continue

                    nhv_20 = row[col_map['nhv_20']].value if 'nhv_20' in col_map and col_map['nhv_20'] < len(row) else None
                    nhv_40 = row[col_map['nhv_40']].value if 'nhv_40' in col_map and col_map['nhv_40'] < len(row) else None
                    hzr_20 = row[col_map['hzr_20']].value if 'hzr_20' in col_map and col_map['hzr_20'] < len(row) else None
                    hzr_40 = row[col_map['hzr_40']].value if 'hzr_40' in col_map and col_map['hzr_40'] < len(row) else None

                    nhv_20_clean = clean_numeric_series(pd.Series([nhv_20]))[0]
                    nhv_40_clean = clean_numeric_series(pd.Series([nhv_40]))[0]
                    hzr_20_clean = clean_numeric_series(pd.Series([hzr_20]))[0]
                    hzr_40_clean = clean_numeric_series(pd.Series([hzr_40]))[0]

                    row_data = {
                        'Nhava Sheva 20': nhv_20,
                        'Nhava Sheva 40HC': nhv_40,
                        'Hazira 20': hzr_20,
                        'Hazira 40HC': hzr_40
                    }
                    print(f"Row {row_idx} in '{sheet_name}': POD={pod}, Rates={row_data}")

                    if pd.notna(nhv_20_clean) or pd.notna(nhv_40_clean):
                        records.append({
                            'POL': 'Nhava Sheva',
                            'PORT': pod.title(),
                            '20': nhv_20_clean,
                            '40STD': None,
                            '40HC': nhv_40_clean,
                            'Remarks': ''
                        })
                    if pd.notna(hzr_20_clean) or pd.notna(hzr_40_clean):
                        records.append({
                            'POL': 'Hazira',
                            'PORT': pod.title(),
                            '20': hzr_20_clean,
                            '40STD': None,
                            '40HC': hzr_40_clean,
                            'Remarks': ''
                        })
                    if not (pd.notna(nhv_20_clean) or pd.notna(nhv_40_clean) or pd.notna(hzr_20_clean) or pd.notna(hzr_40_clean)):
                        print(f"Skipping row {row_idx} in sheet '{sheet_name}': No valid rates (Nhava Sheva: 20={nhv_20}, 40HC={nhv_40}, Hazira: 20={hzr_20}, 40HC={hzr_40})")

                df_final = pd.DataFrame(records)
                if df_final.empty:
                    print(f"No valid records parsed from sheet '{sheet_name}'")
                    return df_final, remarks

                pol_priority = {'Nhava Sheva': 1, 'Hazira': 2}
                df_final['POL_ORDER'] = df_final['POL'].map(pol_priority)
                df_final = df_final.sort_values(by='POL_ORDER').drop(columns='POL_ORDER')
                df_final = df_final[['POL', 'PORT', '20', '40STD', '40HC', 'Remarks']]
                print(f"Parsed {len(df_final)} records from sheet '{sheet_name}', collected {len(remarks)} remarks")
                return df_final, remarks

            except Exception as e:
                print(f"Error in process_turkey_med_sheet for sheet '{sheet_name}': {str(e)}")
                print(f"Full traceback: {traceback.format_exc()}")
                return fallback_parse_turkey_med(file, sheet_name, remarks)

        def fallback_parse_turkey_med(file, sheet_name, remarks):
            print(f"Attempting fallback parsing for sheet '{sheet_name}' using pandas")
            try:
                df_raw = pd.read_excel(file, sheet_name=sheet_name, header=None)
                print(f"Read {len(df_raw)} rows from sheet '{sheet_name}' with pandas")
                
                records = []
                header_row_idx = None
                col_map = {}

                for row_idx, row in df_raw.head(20).iterrows():
                    row_vals = [str(val).strip().upper() for val in row if pd.notna(val) and str(val).strip()]
                    row_str = ' '.join(row_vals)
                    has_keywords = any(x in row_str for x in ['NHAVA', 'NS', 'NSA', 'NHV', 'INNHV', 'HAZIRA', 'HZ', 'INHZA', 'POD', 'PORT', 'DEST', 'DESTINATION', 'DISCHARGE', 'DISCHARGE PORT', 'DEST. PORT', '20', '40', 'HC'])
                    if has_keywords:
                        header_row_idx = row_idx
                        header_row = row
                        break

                if header_row_idx is None:
                    print(f"Fallback: No header row found in sheet '{sheet_name}' after searching 20 rows")
                    for idx, row in df_raw.head(50).iterrows():
                        print(f"Row {idx+1}: {[str(val) for val in row]}")
                    return data_driven_parse_turkey_med(df_raw, sheet_name, remarks)

                print(f"Fallback: Found header row {header_row_idx+1} in '{sheet_name}': {[str(val) for val in header_row]}")
                row_vals = [str(val).strip().upper() for val in header_row if pd.notna(val)]
                pol_cols = {}
                if any(pol in ' '.join(row_vals) for pol in ['INNHV', 'INHZA', 'NHAVA', 'HAZIRA']):
                    if header_row_idx + 1 < len(df_raw):
                        next_row = df_raw.iloc[header_row_idx + 1]
                        next_row_vals = [str(val).strip().upper() for val in next_row if pd.notna(val)]
                        print(f"Fallback: Checking next row {header_row_idx+2} for container types: {[str(val) for val in next_row]}")
                        if any(ct in ' '.join(next_row_vals) for ct in ['20', '40', 'HC']):
                            pol_row = header_row
                            container_row = next_row
                            header_row_idx += 1
                            for col_idx, (pol_val, cont_val) in enumerate(zip(pol_row, container_row)):
                                pol_val = str(pol_val).strip().upper() if pd.notna(pol_val) else ''
                                cont_val = str(cont_val).strip().upper() if pd.notna(cont_val) else ''
                                if 'INNHV' in pol_val or 'NHAVA' in pol_val:
                                    if '20' in cont_val:
                                        col_map['nhv_20'] = col_idx
                                        pol_cols[col_idx] = 'Nhava Sheva'
                                    elif '40' in cont_val or 'HC' in cont_val:
                                        col_map['nhv_40'] = col_idx
                                        pol_cols[col_idx] = 'Nhava Sheva'
                                elif 'INHZA' in pol_val or 'HAZIRA' in pol_val:
                                    if '20' in cont_val:
                                        col_map['hzr_20'] = col_idx
                                        pol_cols[col_idx] = 'Hazira'
                                    elif '40' in cont_val or 'HC' in cont_val:
                                        col_map['hzr_40'] = col_idx
                                        pol_cols[col_idx] = 'Hazira'
                            print(f"Fallback: Updated column mapping after container row: {col_map}")

                for col_idx, val in enumerate(header_row):
                    val = str(val).strip().upper() if pd.notna(val) else ''
                    if any(p in val for p in ['POD', 'PORT', 'DEST', 'DESTINATION', 'DISCHARGE', 'DISCHARGE PORT', 'DEST. PORT']):
                        col_map['pod'] = col_idx
                        break

                if 'pod' not in col_map:
                    for row_idx, row in df_raw.iloc[header_row_idx+1:header_row_idx+10].iterrows():
                        for col_idx, val in enumerate(row):
                            val = str(val).strip() if pd.notna(val) else ''
                            if not val:
                                continue
                            is_numeric = bool(re.match(r'^-?\d+(\.\d+)?$', val.replace(',', '').replace(' ', '')))
                            is_pol = any(pol in val.upper() for pol in ['INNHV', 'INHZA', 'NHAVA', 'HAZIRA', 'NS', 'NSA', 'NHV', 'HZ'])
                            is_container = any(ct in val.upper() for ct in ['20', '40', 'HC'])
                            if not is_numeric and not is_pol and not is_container and len(val) > 1:
                                col_map['pod'] = col_idx
                                print(f"Fallback: Inferred POD column {col_idx} based on value '{val}'")
                                break
                        if 'pod' in col_map:
                            break

                if 'pod' not in col_map:
                    print(f"Fallback: No POD column identified in sheet '{sheet_name}'")
                    for idx, row in df_raw.head(50).iterrows():
                        print(f"Row {idx+1}: {[str(val) for val in row]}")
                    return data_driven_parse_turkey_med(df_raw, sheet_name, remarks)

                print(f"Fallback column mapping for '{sheet_name}': {col_map}")

                for row_idx, row in df_raw.iloc[header_row_idx+1:].iterrows():
                    pod = str(row[col_map['pod']]).strip() if col_map['pod'] < len(row) and pd.notna(row[col_map['pod']]) else ''
                    if not pod:
                        print(f"Fallback: Skipping row {row_idx+1} in sheet '{sheet_name}': Empty POD")
                        continue
                    if len(pod.split()) > 3:
                        remarks.append(pod)
                        continue

                    nhv_20 = row[col_map['nhv_20']] if 'nhv_20' in col_map and col_map['nhv_20'] < len(row) else None
                    nhv_40 = row[col_map['nhv_40']] if 'nhv_40' in col_map and col_map['nhv_40'] < len(row) else None
                    hzr_20 = row[col_map['hzr_20']] if 'hzr_20' in col_map and col_map['hzr_20'] < len(row) else None
                    hzr_40 = row[col_map['hzr_40']] if 'hzr_40' in col_map and col_map['hzr_40'] < len(row) else None

                    nhv_20_clean = clean_numeric_series(pd.Series([nhv_20]))[0]
                    nhv_40_clean = clean_numeric_series(pd.Series([nhv_40]))[0]
                    hzr_20_clean = clean_numeric_series(pd.Series([hzr_20]))[0]
                    hzr_40_clean = clean_numeric_series(pd.Series([hzr_40]))[0]

                    row_data = {
                        'Nhava Sheva 20': nhv_20,
                        'Nhava Sheva 40HC': nhv_40,
                        'Hazira 20': hzr_20,
                        'Hazira 40HC': hzr_40
                    }
                    print(f"Fallback: Row {row_idx+1} in '{sheet_name}': POD={pod}, Rates={row_data}")

                    if pd.notna(nhv_20_clean) or pd.notna(nhv_40_clean):
                        records.append({
                            'POL': 'Nhava Sheva',
                            'PORT': pod.title(),
                            '20': nhv_20_clean,
                            '40STD': None,
                            '40HC': nhv_40_clean,
                            'Remarks': ''
                        })
                    if pd.notna(hzr_20_clean) or pd.notna(hzr_40_clean):
                        records.append({
                            'POL': 'Hazira',
                            'PORT': pod.title(),
                            '20': hzr_20_clean,
                            '40STD': None,
                            '40HC': hzr_40_clean,
                            'Remarks': ''
                        })
                    if not (pd.notna(nhv_20_clean) or pd.notna(nhv_40_clean) or pd.notna(hzr_20_clean) or pd.notna(hzr_40_clean)):
                        print(f"Fallback: Skipping row {row_idx+1} in sheet '{sheet_name}': No valid rates (Nhava Sheva: 20={nhv_20}, 40HC={nhv_40}, Hazira: 20={hzr_20}, 40HC={hzr_40})")

                df_final = pd.DataFrame(records)
                if df_final.empty:
                    print(f"Fallback: No valid records parsed from sheet '{sheet_name}'")
                    return df_final, remarks

                pol_priority = {'Nhava Sheva': 1, 'Hazira': 2}
                df_final['POL_ORDER'] = df_final['POL'].map(pol_priority)
                df_final = df_final.sort_values(by='POL_ORDER').drop(columns='POL_ORDER')
                df_final = df_final[['POL', 'PORT', '20', '40STD', '40HC', 'Remarks']]
                print(f"Fallback: Parsed {len(df_final)} records from sheet '{sheet_name}', collected {len(remarks)} remarks")
                return df_final, remarks
            except Exception as e:
                print(f"Fallback parsing failed for sheet '{sheet_name}': {str(e)}")
                print(f"Full traceback: {traceback.format_exc()}")
                return data_driven_parse_turkey_med(df_raw, sheet_name, remarks)

        def data_driven_parse_turkey_med(df_raw, sheet_name, remarks):
            print(f"Attempting data-driven parsing for sheet '{sheet_name}'")
            records = []
            col_map = {}

            for col_idx in range(min(df_raw.shape[1], 10)):
                column = df_raw.iloc[:, col_idx].dropna().astype(str).str.strip()
                non_numeric_count = 0
                total_count = 0
                for val in column:
                    val_upper = val.upper()
                    is_numeric = bool(re.match(r'^-?\d+(\.\d+)?$', val.replace(',', '').replace(' ', '')))
                    is_pol = any(pol in val_upper for pol in ['INNHV', 'INHZA', 'NHAVA', 'HAZIRA', 'NS', 'NSA', 'NHV', 'HZ'])
                    is_container = any(ct in val_upper for ct in ['20', '40', 'HC'])
                    if not is_numeric and not is_pol and not is_container and len(val) > 1:
                        non_numeric_count += 1
                    total_count += 1
                if total_count > 0 and (non_numeric_count / total_count) > 0.5:
                    col_map['pod'] = col_idx
                    print(f"Data-driven: Inferred POD column {col_idx} based on non-numeric values")
                    break

            if 'pod' not in col_map:
                print(f"Data-driven: No POD column identified in sheet '{sheet_name}'")
                for idx, row in df_raw.head(50).iterrows():
                    print(f"Row {idx+1}: {[str(val) for val in row]}")
                return pd.DataFrame(columns=['POL', 'PORT', '20', '40STD', '40HC', 'Remarks']), remarks + [f"Data-driven parsing failed: No POD column in '{sheet_name}'"]

            pol_cols = {}
            for col_idx in range(df_raw.shape[1]):
                column = df_raw.iloc[:, col_idx].dropna().astype(str).str.strip().str.upper()
                if any('NHAVA' in val or 'NS' in val or 'NSA' in val or 'INNHV' in val for val in column):
                    for offset in range(1, 3):
                        if col_idx + offset >= df_raw.shape[1]:
                            break
                        next_col = df_raw.iloc[:, col_idx + offset].dropna().astype(str).str.strip().str.upper()
                        is_rate_col = any(bool(re.match(r'^-?\d+(\.\d+)?$', val.replace(',', ''))) for val in next_col)
                        if is_rate_col:
                            if any('20' in val for val in next_col):
                                col_map['nhv_20'] = col_idx + offset
                                pol_cols[col_idx + offset] = 'Nhava Sheva'
                            elif any('40' in val or 'HC' in val for val in next_col):
                                col_map['nhv_40'] = col_idx + offset
                                pol_cols[col_idx + offset] = 'Nhava Sheva'
                elif any('HAZIRA' in val or 'HZ' in val or 'INHZA' in val for val in column):
                    for offset in range(1, 3):
                        if col_idx + offset >= df_raw.shape[1]:
                            break
                        next_col = df_raw.iloc[:, col_idx + offset].dropna().astype(str).str.strip().str.upper()
                        is_rate_col = any(bool(re.match(r'^-?\d+(\.\d+)?$', val.replace(',', ''))) for val in next_col)
                        if is_rate_col:
                            if any('20' in val for val in next_col):
                                col_map['hzr_20'] = col_idx + offset
                                pol_cols[col_idx + offset] = 'Hazira'
                            elif any('40' in val or 'HC' in val for val in next_col):
                                col_map['hzr_40'] = col_idx + offset
                                pol_cols[col_idx + offset] = 'Hazira'

            print(f"Data-driven column mapping for '{sheet_name}': {col_map}")

            for row_idx, row in df_raw.iterrows():
                pod = str(row[col_map['pod']]).strip() if col_map['pod'] < len(row) and pd.notna(row[col_map['pod']]) else ''
                is_numeric = bool(re.match(r'^-?\d+(\.\d+)?$', pod.replace(',', '').replace(' ', '')))
                is_pol = any(pol in pod.upper() for pol in ['INNHV', 'INHZA', 'NHAVA', 'HAZIRA', 'NS', 'NSA', 'NHV', 'HZ'])
                is_container = any(ct in pod.upper() for ct in ['20', '40', 'HC'])
                if not pod or is_numeric or is_pol or is_container:
                    print(f"Data-driven: Skipping row {row_idx+1} in sheet '{sheet_name}': Invalid or empty POD '{pod}'")
                    continue
                if len(pod.split()) > 3:
                    remarks.append(pod)
                    continue

                nhv_20 = row[col_map['nhv_20']] if 'nhv_20' in col_map and col_map['nhv_20'] < len(row) else None
                nhv_40 = row[col_map['nhv_40']] if 'nhv_40' in col_map and col_map['nhv_40'] < len(row) else None
                hzr_20 = row[col_map['hzr_20']] if 'hzr_20' in col_map and col_map['hzr_20'] < len(row) else None
                hzr_40 = row[col_map['hzr_40']] if 'hzr_40' in col_map and col_map['hzr_40'] < len(row) else None

                nhv_20_clean = clean_numeric_series(pd.Series([nhv_20]))[0]
                nhv_40_clean = clean_numeric_series(pd.Series([nhv_40]))[0]
                hzr_20_clean = clean_numeric_series(pd.Series([hzr_20]))[0]
                hzr_40_clean = clean_numeric_series(pd.Series([hzr_40]))[0]

                row_data = {
                    'Nhava Sheva 20': nhv_20,
                    'Nhava Sheva 40HC': nhv_40,
                    'Hazira 20': hzr_20,
                    'Hazira 40HC': hzr_40
                }
                print(f"Data-driven: Row {row_idx+1} in sheet '{sheet_name}': POD={pod}, Rates={row_data}")

                if pd.notna(nhv_20_clean) or pd.notna(nhv_40_clean):
                    records.append({
                        'POL': 'Nhava Sheva',
                        'PORT': pod.title(),
                        '20': nhv_20_clean,
                        '40STD': None,
                        '40HC': nhv_40_clean,
                        'Remarks': ''
                    })
                if pd.notna(hzr_20_clean) or pd.notna(hzr_40_clean):
                    records.append({
                        'POL': 'Hazira',
                        'PORT': pod.title(),
                        '20': hzr_20_clean,
                        '40STD': None,
                        '40HC': hzr_40_clean,
                        'Remarks': ''
                    })
                if not (pd.notna(nhv_20_clean) or pd.notna(nhv_40_clean) or pd.notna(hzr_20_clean) or pd.notna(hzr_40_clean)):
                    print(f"Data-driven: Skipping row {row_idx+1} in sheet '{sheet_name}': No valid rates (Nhava Sheva: 20={nhv_20}, 40HC={nhv_40}, Hazira: 20={hzr_20}, 40HC={hzr_40})")

            df_final = pd.DataFrame(records)
            if df_final.empty:
                print(f"Data-driven: No valid records parsed from sheet '{sheet_name}'")
                return df_final, remarks

            pol_priority = {'Nhava Sheva': 1, 'Hazira': 2}
            df_final['POL_ORDER'] = df_final['POL'].map(pol_priority)
            df_final = df_final.sort_values(by='POL_ORDER').drop(columns='POL_ORDER')
            df_final = df_final[['POL', 'PORT', '20', '40STD', '40HC', 'Remarks']]
            print(f"Data-driven: Parsed {len(df_final)} records from sheet '{sheet_name}', collected {len(remarks)} remarks")
            return df_final, remarks

        def process_generic_sheet(sheet_name, pol_name):
            remarks = []
            try:
                df_raw = pd.read_excel(file, sheet_name=sheet_name, header=None)
                print(f"Processing sheet '{sheet_name}' with {len(df_raw)} rows")
                df_raw = df_raw.iloc[4:].copy()
                df_raw.columns = ['PORT', '20', '40HC', 'Remarks', 'DG_20', 'DG_40']
                data_rows = []
                for idx, row in df_raw.iterrows():
                    port = str(row['PORT']).strip() if pd.notna(row['PORT']) else ''
                    if not port:
                        continue
                    rate_20 = row['20']
                    rate_40hc = row['40HC']
                    if pd.isna(rate_20) and pd.isna(rate_40hc):
                        print(f"Skipping row {idx} in sheet '{sheet_name}': Both '20' and '40HC' are NaN")
                        continue
                    if len(port.split()) <= 3:
                        data_rows.append(row)
                    elif len(port.split()) > 3:
                        remarks.append(port.strip())
                if not data_rows:
                    print(f"No valid data rows in sheet '{sheet_name}'")
                    return pd.DataFrame(columns=['POL', 'PORT', '20', '40STD', '40HC', 'Remarks']), remarks
                df_data = pd.DataFrame(data_rows)
                df_data['POL'] = pol_name
                df_data['PORT'] = df_data['PORT'].astype(str).str.strip()
                print(f"Sheet '{sheet_name}' rate columns sample: {df_data[['20', '40HC']].head().to_dict()}")
                df_data['20'] = clean_numeric_series(df_data['20'])
                df_data['40HC'] = clean_numeric_series(df_data['40HC'])
                df_data['Remarks'] = df_data['Remarks'].astype(str).str.strip().replace('nan', '')
                df_data['40STD'] = np.nan
                parsed_df = df_data[['POL', 'PORT', '20', '40STD', '40HC', 'Remarks']]
                parsed_df = parsed_df.dropna(subset=['PORT'])
                parsed_df = parsed_df[parsed_df['PORT'].str.strip() != '']
                parsed_df = parsed_df[~(parsed_df['20'].isna() & parsed_df['40HC'].isna())]
                print(f"Parsed {len(parsed_df)} rows from sheet '{sheet_name}', collected {len(remarks)} remarks")
                return parsed_df, remarks
            except Exception as e:
                print(f"Error in process_generic_sheet for sheet '{sheet_name}': {str(e)}")
                return pd.DataFrame(columns=['POL', 'PORT', '20', '40STD', '40HC', 'Remarks']), remarks + [f"Error in '{sheet_name}': {str(e)}"]

        def process_africa_sheet(sheet_name):
            remarks = []
            try:
                df_raw = pd.read_excel(file, sheet_name=sheet_name, header=None)
                table_start_row = None
                for idx, row in df_raw.iterrows():
                    row_str = ' '.join(row.dropna().astype(str).str.upper())
                    if "MUNDRA" in row_str or "NHAVA SHEVA" in row_str or "PIPAVAV" in row_str:
                        table_start_row = idx
                        break
                if table_start_row is None:
                    print(f"Could not detect table start in '{sheet_name}' sheet")
                    return pd.DataFrame(columns=['POL', 'PORT', '20', '40STD', '40HC', 'Remarks']), [f"Could not detect table start in '{sheet_name}' sheet."]
                for row_idx in range(table_start_row):
                    row = df_raw.iloc[row_idx]
                    for cell in row:
                        if pd.notna(cell) and str(cell).strip():
                            remarks.append(str(cell).strip())
                known_ports = ["APAPA", "TINCAN", "TEMA", "ABIDJAN", "COTONOU", "CONTONU", "LOME", "LEKKI", "ONNE", "MOMBASA", "DAR ES SALAAM"]
                port_row_idx = None
                for idx in range(table_start_row + 1, len(df_raw)):
                    row = df_raw.iloc[idx]
                    row_str = ' '.join(row.dropna().astype(str).str.upper())
                    if any(port in row_str for port in known_ports):
                        port_row_idx = idx
                        break
                if port_row_idx is None:
                    print(f"Could not detect PORT row in '{sheet_name}' sheet")
                    return pd.DataFrame(columns=['POL', 'PORT', '20', '40STD', '40HC', 'Remarks']), [f"Could not detect PORT row in '{sheet_name}' sheet."]
                for row_idx in range(table_start_row + 1, port_row_idx):
                    row = df_raw.iloc[row_idx]
                    for cell in row:
                        if pd.notna(cell) and str(cell).strip():
                            remarks.append(str(cell).strip())
                pol_row = df_raw.iloc[table_start_row].dropna().astype(str).str.upper()
                port_row = df_raw.iloc[port_row_idx].dropna().astype(str)
                header_row = df_raw.iloc[port_row_idx + 1].dropna().astype(str)
                pols = [standardize_pol(p.strip()) for p in pol_row.iloc[0].split('/')] if not pol_row.empty else []
                ports = port_row.tolist()
                headers = header_row.tolist()
                if len(ports) == 0 or len(headers) == 0:
                    print(f"No PORTs or headers in '{sheet_name}' sheet")
                    return pd.DataFrame(columns=['POL', 'PORT', '20', '40STD', '40HC', 'Remarks']), [f"No PORTs or headers in '{sheet_name}' sheet."]
                if "west africa" in sheet_name.lower().strip():
                    if len(headers) % 2 != 0 or len(headers) // 2 != len(ports):
                        print(f"Inconsistent PORT or header structure in '{sheet_name}' sheet")
                        return pd.DataFrame(columns=['POL', 'PORT', '20', '40STD', '40HC', 'Remarks']), [f"Inconsistent PORT or header structure in '{sheet_name}' sheet."]
                else:
                    if len(ports) * 2 != len(headers):
                        print(f"Inconsistent PORT or header structure in '{sheet_name}' sheet")
                        return pd.DataFrame(columns=['POL', 'PORT', '20', '40STD', '40HC', 'Remarks']), [f"Inconsistent PORT or header structure in '{sheet_name}' sheet."]
                data_start_row = port_row_idx + 2
                while data_start_row < len(df_raw):
                    data_row = df_raw.iloc[data_start_row]
                    if any(pd.to_numeric(str(cell), errors='coerce') == cell for cell in data_row.iloc[:len(headers)] if pd.notna(cell)):
                        break
                    for cell in data_row:
                        if pd.notna(cell) and str(cell).strip():
                            remarks.append(str(cell).strip())
                    data_start_row += 1
                if data_start_row >= len(df_raw):
                    print(f"No data rows found in '{sheet_name}' sheet")
                    return pd.DataFrame(columns=['POL', 'PORT', '20', '40STD', '40HC', 'Remarks']), remarks
                data_rows = df_raw.iloc[data_start_row:data_start_row + 1]
                if data_rows.empty or len(data_rows.columns) < len(headers):
                    print(f"No valid data rows or insufficient columns in '{sheet_name}' sheet")
                    return pd.DataFrame(columns=['POL', 'PORT', '20', '40STD', '40HC', 'Remarks']), remarks
                data_frames = []
                for port_idx, port in enumerate(ports):
                    dv20_value = data_rows.iloc[0, 2 * port_idx] if 2 * port_idx < len(data_rows.columns) else None
                    hc40_value = data_rows.iloc[0, 2 * port_idx + 1] if 2 * port_idx + 1 < len(data_rows.columns) else None
                    dv20_clean = clean_numeric_series(pd.Series([dv20_value]))[0]
                    hc40_clean = clean_numeric_series(pd.Series([hc40_value]))[0]
                    if pd.isna(dv20_clean) and pd.isna(hc40_clean):
                        continue
                    for pol in pols:
                        df_pol_port = pd.DataFrame({
                            'POL': [pol],
                            'PORT': [port.title()],
                            '20': [dv20_clean],
                            '40HC': [hc40_clean],
                            '40STD': [np.nan],
                            'Remarks': ['']
                        })
                        data_frames.append(df_pol_port)
                parsed_df = pd.concat(data_frames, ignore_index=True) if data_frames else pd.DataFrame(columns=['POL', 'PORT', '20', '40STD', '40HC', 'Remarks'])
                parsed_df['POL'] = parsed_df['POL'].apply(standardize_pol)
                parsed_df = parsed_df[parsed_df['PORT'].str.strip() != '']
                print(f"Parsed {len(parsed_df)} rows from sheet '{sheet_name}', collected {len(remarks)} remarks")
                return parsed_df, remarks
            except Exception as e:
                print(f"Error in process_africa_sheet for sheet '{sheet_name}': {str(e)}")
                return pd.DataFrame(columns=['POL', 'PORT', '20', '40STD', '40HC', 'Remarks']), remarks + [f"Error in '{sheet_name}': {str(e)}"]

        def process_australia_sheet(sheet_name):
            remarks = []
            try:
                df_raw = pd.read_excel(file, sheet_name=sheet_name, header=None)
                records = []
                current_pod_group = None
                reading_block = False
                for idx, row in df_raw.iterrows():
                    row_vals = [cell for cell in row]
                    if pd.notna(row_vals[0]) and isinstance(row_vals[1], str):
                        if "SYDNEY" in row_vals[1].upper():
                            current_pod_group = "SYDNEY/MELBOURNE/BRISBANE"
                            reading_block = False
                            continue
                        elif "AKL" in row_vals[1].upper():
                            current_pod_group = "AKL/TAU"
                            reading_block = False
                            continue
                    if row_vals[0] == "POL":
                        reading_block = True
                        continue
                    pol_raw = str(row_vals[0]).strip() if pd.notna(row_vals[0]) else ""
                    if reading_block and pol_raw in ["Nhava Sheva", "Mundra", "Hazira"]:
                        rate_20 = clean_numeric_series(pd.Series([row_vals[1]]))[0] if pd.notna(row_vals[1]) else None
                        rate_40 = clean_numeric_series(pd.Series([row_vals[2]]))[0] if pd.notna(row_vals[2]) else None
                        if pd.isna(rate_20) and pd.isna(rate_40):
                            continue
                        for pod in current_pod_group.split("/"):
                            pod_clean = pod.strip().title()
                            records.append({
                                'POL': pol_raw,
                                'PORT': pod_clean,
                                '20': rate_20,
                                '40HC': rate_40,
                                '40STD': None,
                                'Remarks': ''
                            })
                        continue
                    if row_vals[0] not in ["Nhava Sheva", "Mundra", "Hazira"] and any(isinstance(val, str) and len(val.strip()) > 5 for val in row_vals if pd.notna(val)):
                        joined = " ".join(str(val).strip() for val in row_vals if pd.notna(val))
                        if joined not in remarks:
                            remarks.append(joined)
                df_final = pd.DataFrame(records)
                if df_final.empty:
                    print(f"No valid records parsed from sheet '{sheet_name}'")
                    return df_final, remarks
                df_final = df_final[['POL', 'PORT', '20', '40STD', '40HC', 'Remarks']]
                print(f"Parsed {len(df_final)} records from sheet '{sheet_name}', collected {len(remarks)} remarks")
                return df_final, remarks
            except Exception as e:
                print(f"Error in process_australia_sheet for sheet '{sheet_name}': {str(e)}")
                return pd.DataFrame(columns=['POL', 'PORT', '20', '40STD', '40HC', 'Remarks']), remarks + [f"Error in '{sheet_name}': {str(e)}"]

        def process_ecsa_sheet(sheet_name):
            remarks = []
            try:
                df_raw = pd.read_excel(file, sheet_name=sheet_name, header=None)
                pol_row_idx = 0
                pol_row = df_raw.iloc[pol_row_idx].dropna().astype(str).str.upper()
                pols = [standardize_pol(p.strip()) for p in pol_row.iloc[0].split('/')] if not pol_row.empty else ["Nhava Sheva", "Mundra"]
                data_start_row = 1
                data_end_row = None
                for idx in range(data_start_row, len(df_raw)):
                    port = str(df_raw.iloc[idx, 0]).strip() if pd.notna(df_raw.iloc[idx, 0]) else ''
                    if not port or any(keyword in port.upper() for keyword in ["ABOVE", "SUB", "DG", "VALIDITY", "ROUTE", "SUBJECT", "INCU", "INKT"]):
                        data_end_row = idx
                        break
                if data_end_row is None:
                    data_end_row = len(df_raw)
                port_data = []
                for idx in range(data_start_row, data_end_row):
                    port = str(df_raw.iloc[idx, 0]).strip() if pd.notna(df_raw.iloc[idx, 0]) else ''
                    if not port:
                        continue
                    rate_20 = df_raw.iloc[idx, 1] if pd.notna(df_raw.iloc[idx, 1]) else None
                    rate_40 = df_raw.iloc[idx, 2] if pd.notna(df_raw.iloc[idx, 2]) else None
                    rate_20_clean = clean_numeric_series(pd.Series([rate_20]))[0]
                    rate_40_clean = clean_numeric_series(pd.Series([rate_40]))[0]
                    if pd.isna(rate_20_clean) and pd.isna(rate_40_clean):
                        continue
                    port_data.append({
                        'PORT': port.title(),
                        '20': rate_20_clean,
                        '40HC': rate_40_clean,
                        'Remarks': ''
                    })
                records = []
                for data in port_data:
                    records.append({
                        'POL': 'Nhava Sheva',
                        'PORT': data['PORT'],
                        '20': data['20'],
                        '40HC': data['40HC'],
                        'Remarks': data['Remarks']
                    })
                for data in port_data:
                    records.append({
                        'POL': 'Mundra',
                        'PORT': data['PORT'],
                        '20': data['20'],
                        '40HC': data['40HC'],
                        'Remarks': data['Remarks']
                    })
                for idx in range(data_end_row, len(df_raw)):
                    row = df_raw.iloc[idx]
                    for cell in row:
                        if pd.notna(cell) and str(cell).strip():
                            remarks.append(str(cell).strip())
                df_final = pd.DataFrame(records)
                if df_final.empty:
                    print(f"No valid records parsed from sheet '{sheet_name}'")
                    return df_final, remarks
                df_final['40STD'] = np.nan
                df_final = df_final[['POL', 'PORT', '20', '40STD', '40HC', 'Remarks']]
                print(f"Parsed {len(df_final)} records from sheet '{sheet_name}', collected {len(remarks)} remarks")
                return df_final, remarks
            except Exception as e:
                print(f"Error in process_ecsa_sheet for sheet '{sheet_name}': {str(e)}")
                return pd.DataFrame(columns=['POL', 'PORT', '20', '40STD', '40HC', 'Remarks']), remarks + [f"Error in '{sheet_name}': {str(e)}"]

        def process_gulf_sheet(sheet_name):
            remarks = []
            try:
                df_raw = pd.read_excel(file, sheet_name=sheet_name, header=None)
                port_row = df_raw.iloc[0].dropna().astype(str)[1:]
                ports = port_row.tolist()
                if not ports:
                    print(f"No PORTs found in '{sheet_name}' sheet")
                    return pd.DataFrame(columns=['POL', 'PORT', '20', '40STD', '40HC', 'Remarks']), [f"No PORTs found in '{sheet_name}' sheet."]
                pol_row_idx = None
                for idx, row in df_raw.iterrows():
                    if str(row[0]).strip().upper() == "POL":
                        pol_row_idx = idx
                        break
                if pol_row_idx is None:
                    print(f"Could not detect POL row in '{sheet_name}' sheet")
                    return pd.DataFrame(columns=['POL', 'PORT', '20', '40STD', '40HC', 'Remarks']), [f"Could not detect POL row in '{sheet_name}' sheet."]
                data_row_idx = pol_row_idx + 1
                data_row = df_raw.iloc[data_row_idx]
                pol = standardize_pol(str(data_row[0]).strip())
                if pol != "Nhava Sheva":
                    pol = "Nhava Sheva"
                records = []
                for port_idx, port in enumerate(ports):
                    col_20_idx = 1 + (port_idx * 2)
                    col_40_idx = col_20_idx + 1
                    rate_20 = data_row[col_20_idx] if col_20_idx < len(data_row) and pd.notna(data_row[col_20_idx]) else None
                    rate_40 = data_row[col_40_idx] if col_40_idx < len(data_row) and pd.notna(data_row[col_40_idx]) else None
                    rate_20_clean = clean_numeric_series(pd.Series([rate_20]))[0]
                    rate_40_clean = clean_numeric_series(pd.Series([rate_40]))[0]
                    if pd.isna(rate_20_clean) and pd.isna(rate_40_clean):
                        continue
                    records.append({
                        'POL': pol,
                        'PORT': port.title(),
                        '20': rate_20_clean,
                        '40HC': rate_40_clean,
                        'Remarks': ''
                    })
                for idx in range(data_row_idx + 1, len(df_raw)):
                    row = df_raw.iloc[idx]
                    for cell in row:
                        if pd.notna(cell) and str(cell).strip():
                            remarks.append(str(cell).strip())
                df_final = pd.DataFrame(records)
                if df_final.empty:
                    print(f"No valid records parsed from sheet '{sheet_name}'")
                    return df_final, remarks
                df_final['40STD'] = np.nan
                df_final = df_final[['POL', 'PORT', '20', '40STD', '40HC', 'Remarks']]
                print(f"Parsed {len(df_final)} records from sheet '{sheet_name}', collected {len(remarks)} remarks")
                return df_final, remarks
            except Exception as e:
                print(f"Error in process_gulf_sheet for sheet '{sheet_name}': {str(e)}")
                return pd.DataFrame(columns=['POL', 'PORT', '20', '40STD', '40HC', 'Remarks']), remarks + [f"Error in '{sheet_name}': {str(e)}"]

        def process_latam_sheet(sheet_name):
            remarks = []
            try:
                df_raw = pd.read_excel(file, sheet_name=sheet_name, header=None)
                records = []
                seen_lines = set()
                def is_record_row(row_vals):
                    return (
                        len(row_vals) >= 5 and
                        row_vals[0] and "/" in str(row_vals[0]) and
                        row_vals[2] and (
                            isinstance(row_vals[3], (int, float)) or
                            (isinstance(row_vals[3], str) and row_vals[3].replace(',', '').replace('.', '').isdigit()) or
                            row_vals[3] == 'NA'
                        ) and (
                            isinstance(row_vals[4], (int, float)) or
                            (isinstance(row_vals[4], str) and row_vals[4].replace(',', '').replace('.', '').isdigit()) or
                            row_vals[4] == 'NA'
                        )
                    )
                for idx, row in df_raw.iterrows():
                    row_vals = row.tolist()
                    full_line = " ".join(str(cell).strip() for cell in row_vals if pd.notna(cell) and str(cell).strip())
                    if is_record_row(row_vals):
                        raw_pol = str(row_vals[0]).strip()
                        port = str(row_vals[2]).strip()
                        rate_20 = clean_numeric_series(pd.Series([row_vals[3]]))[0]
                        rate_40 = clean_numeric_series(pd.Series([row_vals[4]]))[0]
                        if pd.isna(rate_20) and pd.isna(rate_40):
                            continue
                        for pol_code in raw_pol.split("/"):
                            pol = standardize_pol(pol_code.strip())
                            if pol not in ["Nhava Sheva", "Mundra", "Hazira"]:
                                continue
                            records.append({
                                'POL': pol,
                                'PORT': port,
                                '20': rate_20,
                                '40HC': rate_40,
                                '40STD': None,
                                'Remarks': ''
                            })
                    elif full_line and full_line not in seen_lines:
                        remarks.append(full_line)
                        seen_lines.add(full_line)
                df_final = pd.DataFrame(records)
                if df_final.empty:
                    print(f"No valid records parsed from sheet '{sheet_name}'")
                    return df_final, remarks
                pol_order = {'Nhava Sheva': 1, 'Mundra': 2, 'Hazira': 3}
                df_final['POL_ORDER'] = df_final['POL'].map(pol_order)
                df_final = df_final.sort_values(by='POL_ORDER').drop(columns='POL_ORDER')
                df_final = df_final[['POL', 'PORT', '20', '40STD', '40HC', 'Remarks']]
                print(f"Parsed {len(df_final)} records from sheet '{sheet_name}', collected {len(remarks)} remarks")
                return df_final, remarks
            except Exception as e:
                print(f"Error in process_latam_sheet for sheet '{sheet_name}': {str(e)}")
                return pd.DataFrame(columns=['POL', 'PORT', '20', '40STD', '40HC', 'Remarks']), remarks + [f"Error in '{sheet_name}': {str(e)}"]

        def process_canada_sheet(sheet_name):
            remarks = []
            try:
                df_raw = pd.read_excel(file, sheet_name=sheet_name, header=None, dtype=str)
                header_row_idx = None
                for idx, row in df_raw.iterrows():
                    if "DV20" in " ".join(row.dropna().astype(str).str.upper()) and "HC40" in " ".join(row.dropna().astype(str).str.upper()):
                        header_row_idx = idx
                        break
                if header_row_idx is None:
                    print(f"Header row not found in '{sheet_name}' sheet")
                    return pd.DataFrame(columns=['POL', 'PORT', '20', '40STD', '40HC', 'Remarks']), [f"Header row not found in '{sheet_name}' sheet."]
                header_row = df_raw.iloc[header_row_idx]
                pol_row = df_raw.iloc[header_row_idx - 1]
                port_start_row = header_row_idx + 1
                all_pols = [standardize_pol(pol) for pol in pol_row if pd.notna(pol) and pol.strip() and standardize_pol(pol) != 'Wrg']
                rate_col_pairs = [(i, i + 1) for i in range(1, len(header_row)-1, 2)
                                if header_row[i].upper() == "DV20" and header_row[i + 1].upper() == "HC40"]
                ports = []
                data_end_row = None
                for idx in range(port_start_row, len(df_raw)):
                    port = str(df_raw.iloc[idx, 0]).strip()
                    if not port or any(word in port.upper() for word in ["INCLUSIVE", "SUBJECT", "NBF", "ZIM", "CN RAIL", "NOTE"]):
                        data_end_row = idx
                        break
                    ports.append(port)
                if data_end_row is None:
                    data_end_row = len(df_raw)
                for idx in range(0, port_start_row):
                    line = " ".join(str(cell).strip() for cell in df_raw.iloc[idx] if pd.notna(cell) and str(cell).strip())
                    if line:
                        remarks.append(line)
                for idx in range(data_end_row, len(df_raw)):
                    line = " ".join(str(cell).strip() for cell in df_raw.iloc[idx] if pd.notna(cell) and str(cell).strip())
                    if line:
                        remarks.append(line)
                records = []
                for pol_idx, pol in enumerate(all_pols):
                    if pol_idx >= len(rate_col_pairs):
                        continue
                    col_20_idx, col_40_idx = rate_col_pairs[pol_idx]
                    for i, port in enumerate(ports):
                        row_idx = port_start_row + i
                        rate_20_raw = df_raw.iloc[row_idx, col_20_idx] if col_20_idx < len(df_raw.columns) else None
                        rate_40_raw = df_raw.iloc[row_idx, col_40_idx] if col_40_idx < len(df_raw.columns) else None
                        rate_20 = clean_numeric_series(pd.Series([rate_20_raw]))[0] if rate_20_raw else None
                        rate_40 = clean_numeric_series(pd.Series([rate_40_raw]))[0] if rate_40_raw else None
                        if pd.notna(rate_20) or pd.notna(rate_40):
                            records.append({
                                'POL': pol,
                                'PORT': port,
                                '20': rate_20,
                                '40HC': rate_40,
                                '40STD': None,
                                'Remarks': ''
                            })
                df_final = pd.DataFrame(records)
                if df_final.empty:
                    print(f"No valid records parsed from sheet '{sheet_name}'")
                    return df_final, remarks
                df_final = df_final[['POL', 'PORT', '20', '40STD', '40HC', 'Remarks']]
                print(f"Parsed {len(df_final)} records from sheet '{sheet_name}', collected {len(remarks)} remarks")
                return df_final, remarks
            except Exception as e:
                print(f"Error in process_canada_sheet for sheet '{sheet_name}': {str(e)}")
                return pd.DataFrame(columns=['POL', 'PORT', '20', '40STD', '40HC', 'Remarks']), remarks + [f"Error in '{sheet_name}': {str(e)}"]

        for sheet_name in sheets:
            sheet_name_lower = sheet_name.lower().strip()
            if 'turkey' in sheet_name_lower and 'med' in sheet_name_lower:
                df, remarks = process_turkey_med_sheet(sheet_name)
            elif 'australia' in sheet_name_lower or 'aus' in sheet_name_lower:
                df, remarks = process_australia_sheet(sheet_name)
            elif 'ecsa' in sheet_name_lower:
                df, remarks = process_ecsa_sheet(sheet_name)
            elif 'gulf' in sheet_name_lower:
                df, remarks = process_gulf_sheet(sheet_name)
            elif 'latam' in sheet_name_lower:
                df, remarks = process_latam_sheet(sheet_name)
            elif 'canada' in sheet_name_lower:
                df, remarks = process_canada_sheet(sheet_name)
            elif 'africa' in sheet_name_lower:
                df, remarks = process_africa_sheet(sheet_name)
            else:
                df, remarks = process_generic_sheet(sheet_name, 'Nhava Sheva')
            if not df.empty:
                data_frames.append(df)
            all_remarks.extend([f"{sheet_name}: {remark}" for remark in remarks if remark and len(remark.split()) > 2])

        if not data_frames:
            print("No data frames parsed from any sheet")
            return pd.DataFrame(columns=['POL', 'PORT', '20', '40STD', '40HC', 'Remarks']), all_remarks

        final_df = pd.concat(data_frames, ignore_index=True)
        final_df['POL'] = final_df['POL'].apply(standardize_pol)
        final_df = final_df[final_df['PORT'].notna() & (final_df['PORT'].str.strip() != '')]
        final_df = final_df.sort_values(by=['POL', 'PORT'])
        all_remarks = list(dict.fromkeys(all_remarks))

        print(f"Total records parsed: {len(final_df)}")
        print(f"Total remarks collected: {len(all_remarks)}")
        return final_df, all_remarks

    except Exception as e:
        print(f"Error parsing ZIM MRG file: {str(e)}")
        print(f"Full traceback: {traceback.format_exc()}")
        return pd.DataFrame(columns=['POL', 'PORT', '20', '40STD', '40HC', 'Remarks']), [f"Error parsing ZIM MRG file: {str(e)}"]
    
# === FIREBASE DATA FUNCTIONS ===
@st.cache_data
def load_from_firestore():
    data = {
        "MSC": {}, "Wan Hai": {}, "Emirates": {}, "ONE MRG": {}, "HMM MRG": {},
        "OOCL": {}, "PIL MRG": {}, "ARKAS MRG": {}, "Interasia": {}, "Cosco-Gulf": {}, 
        "Cosco-WCSA & CB": {}, "Cosco-Africa": {}, "Turkon": {}, "Cosco-Fareast": {}, "ZIM MRG": {}
    }
    all_pods = set()
    all_pols = set()
    vendors_ref = db.collection('vendors')
    for vendor in data.keys():
        docs = vendors_ref.document(vendor).collection('data').stream()
        for doc in docs:
            data[vendor][doc.id] = doc.to_dict()
            df_temp = pd.DataFrame(doc.to_dict().get("data", []))
            if 'PORT' in df_temp.columns:
                all_pods.update(df_temp['PORT'].dropna().astype(str).str.strip().unique())
            if 'POD' in df_temp.columns:  # Handle HMM MRG case before renaming
                all_pods.update(df_temp['POD'].dropna().astype(str).str.strip().unique())
            if 'POL' in df_temp.columns:
                df_temp['POL'] = df_temp['POL'].apply(standardize_pol)  # Standardize POL names
                all_pols.update(df_temp['POL'].dropna().astype(str).str.strip().unique())
    return data, sorted(list(all_pods)), sorted(list(all_pols))

def save_to_firestore(vendor, month_year, df, info):
    # Standardize POL names before saving
    if 'POL' in df.columns:
        df['POL'] = df['POL'].apply(standardize_pol)
    doc_ref = db.collection('vendors').document(vendor).collection('data').document(month_year)
    batch = db.batch()
    data_dict = {'data': df.to_dict(orient='records'), 'info': info}
    batch.set(doc_ref, data_dict, merge=True)
    batch.commit()

def query_firestore(pol, pod, equipment):
    results = []
    vendors = ["MSC", "Wan Hai", "Emirates", "ONE MRG", "HMM MRG", "OOCL", "PIL MRG", "ARKAS MRG", "Interasia", "Cosco-Gulf", "Cosco-WCSA & CB", "Cosco-Africa", "Turkon", "Cosco-Fareast", "ZIM MRG"]
    for vendor in vendors:
        vendor_ref = db.collection('vendors').document(vendor).collection('data')
        docs = vendor_ref.stream()
        for doc in docs:
            doc_data = doc.to_dict()
            df = pd.DataFrame(doc_data.get("data", []))
            if df.empty:
                continue

            # Standardize POL names in the DataFrame
            if 'POL' in df.columns:
                df['POL'] = df['POL'].apply(standardize_pol)

            # Apply POL filter
            if 'POL' in df.columns and pol:
                df = df[df['POL'].str.lower().str.contains(pol.lower(), na=False)]

            # Apply POD filter
            if 'PORT' in df.columns and pod:
                df = df[df['PORT'].str.lower().str.contains(pod.lower(), na=False)]
                # Ensure PORT is not empty or "None"
                df = df[df['PORT'].ne('') & df['PORT'].ne('None')]
            elif 'POD' in df.columns and pod:  # Handle HMM MRG case
                df = df[df['POD'].str.lower().str.contains(pod.lower(), na=False)]
                df = df[df['POD'].ne('') & df['POD'].ne('None')]
                if not df.empty:
                    df = df.rename(columns={'POD': 'PORT'})  # Rename POD to PORT for display

            # Apply equipment filter and ensure non-"None" values
            if equipment and any(eq in df.columns for eq in equipment):
                # Check for non-NaN and non-"None" values in equipment columns
                conditions = []
                for eq in equipment:
                    if eq in df.columns:
                        # Exclude rows where the equipment value is NaN, empty, or "None"
                        condition = df[eq].notna() & (df[eq] != '') & (df[eq] != 'None')
                        conditions.append(condition)
                if conditions:
                    # Require at least one equipment column to have a valid value
                    df = df[pd.concat([pd.Series(c) for c in conditions], axis=1).any(axis=1)]

            # Additional check: Ensure all selected equipment columns have valid data (not "None")
            if equipment:
                for eq in equipment:
                    if eq in df.columns:
                        df = df[df[eq].ne('None')]

            # Only append if DataFrame has valid data after filtering
            if not df.empty:
                # Ensure PORT and equipment columns are not "None"
                if 'PORT' in df.columns:
                    df = df[df['PORT'].ne('None')]
                if df.empty:
                    continue  # Skip if PORT filter removes all rows

                df['Month-Year'] = doc.id
                df['Vendor'] = vendor
                results.append(df)

    return pd.concat(results, ignore_index=True) if results else pd.DataFrame()

def delete_from_firestore(vendor, month_year):
    db.collection('vendors').document(vendor).collection('data').document(month_year).delete()

# === STREAMLIT APP ===
st.set_page_config(page_title="Vendor Data Parser", layout="wide")

# Apply custom CSS with sophisticated color theme
st.markdown(
    """
    <style>
    .stApp {background: linear-gradient(135deg, #f0f4f8, #ffffff); color: #2c3e50; font-family: 'Helvetica Neue', sans-serif;}
    .stTitle {text-align: center; font-size: 2.5em; color: #1a252f; text-shadow: 1px 1px 3px rgba(0,0,0,0.1); margin-bottom: 30px;}
    .stSubheader {color: #34495e; font-weight: 600; margin-bottom: 15px; text-transform: uppercase; letter-spacing: 1px;}
    .section-container {background: #ffffff; border-radius: 12px; padding: 20px; margin-bottom: 20px; box-shadow: 0 4px 8px rgba(0,0,0,0.1); border: 1px solid #ecf0f1;}
    .stButton>button {background: linear-gradient(45deg, #3498db, #8e44ad); color: white; border: none; padding: 10px 20px; border-radius: 6px; font-weight: 500; transition: all 0.3s ease; box-shadow: 0 2px 4px rgba(0,0,0,0.1);}
    .stButton>button:hover {background: linear-gradient(45deg, #2980b9, #8e44ad); transform: translateY(-2px); box-shadow: 0 4px 8px rgba(0,0,0,0.2);}
    .stSelectbox, .stFileUploader {background-color: #ffffff; border: 1px solid #bdc3c7; border-radius: 6px; padding: 8px; box-shadow: inset 0 1px 2px rgba(0,0,0,0.05);}
    .stSelectbox div[data-baseweb="select"] {border: 1px solid #bdc3c7; border-radius: 6px;}
    .stCheckbox {margin-right: 15px;}
    .equipment-container {display: flex; align-items: center; margin-top: 5px;}
    .equipment-container label {margin-right: 20px; font-size: 14px; color: #34495e;}
    .search-row {display: flex; align-items: center; gap: 15px;}
    .search-row .stSelectbox {flex: 1; min-width: 150px;}
    .search-row .equipment-container {flex: 1; min-width: 300px;}
    .search-row .stButton {flex: 0;}
    .vendor-button {width: 100%; text-align: center; background: #ecf0f1; border-radius: 6px; padding: 10px; transition: background 0.3s ease;}
    .vendor-button:hover {background: #dfe6ea;}
    .horizontal-checkboxes {display: flex; gap: 20px; align-items: center;}
    .horizontal-checkboxes label {margin-right: 0; font-size: 14px; color: #34495e;}
    </style>
    """,
    unsafe_allow_html=True
)

# Initialize session state
if 'page' not in st.session_state:
    st.session_state.page = 'main'
if 'selected_vendor' not in st.session_state:
    st.session_state.selected_vendor = None
if 'pod_suggestions' not in st.session_state or 'pol_suggestions' not in st.session_state:
    _, st.session_state.pod_suggestions, pol_suggestions_from_db = load_from_firestore()
    st.session_state.pod_suggestions = sorted(st.session_state.pod_suggestions)
    mandatory_pols = {"Nhava Sheva", "Rajula", "Pipavav"}
    st.session_state.pol_suggestions = sorted(list(set(pol_suggestions_from_db) | mandatory_pols))
if 'search_results' not in st.session_state:
    st.session_state.search_results = pd.DataFrame()

def main_page():
    # === Title ===
    st.title("Vendor Data Parser")

    # === Upload Vendor Data Section ===
    st.subheader("Upload Vendor Data")
    col1, col2, col3 = st.columns(3)
    with col1:
        month = st.selectbox("Month", ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"])
    with col2:
        year = st.selectbox("Year", [2025, 2026, 2027, 2028, 2029, 2030], format_func=lambda x: str(x), help="Select the year for the data.")
    with col3:
        vendor = st.selectbox("Vendor", ["MSC", "Wan Hai", "Emirates", "ONE MRG", "HMM MRG", "OOCL", "PIL MRG", "ARKAS MRG", "Interasia", "Cosco-Gulf", "Cosco-WCSA & CB", "Cosco-Africa", "Turkon", "Cosco-Fareast", "ZIM MRG"])
    month_year = f"{year}-{month[:3].upper()}"
    uploaded_file = st.file_uploader("Upload Excel or PDF", type=['xlsx', 'pdf'])

    if uploaded_file and st.button("Parse and Store"):
        parser_map = {
            "MSC": parse_msc,
            "Wan Hai": parse_wan_hai,
            "Emirates": parse_emirates,
            "ONE MRG": parse_one,
            "HMM MRG": parse_hmm,
            "OOCL": parse_oocl,
            "PIL MRG": parse_pil,
            "ARKAS MRG": parse_arkas,
            "Interasia": parse_interasia,
            "Cosco-Gulf": parse_cosco_gulf,
            "Cosco-WCSA & CB": parse_cosco_wcsa_cb,
            "Cosco-Africa": parse_cosco_africa,
            "Turkon": parse_turkon,
            "Cosco-Fareast": parse_cosco_fareast,
            "ZIM MRG": parse_zim
        }
        try:
            df, info = parser_map[vendor](uploaded_file, month_year)
            if not df.empty:
                save_to_firestore(vendor, month_year, df, info)
                st.cache_data.clear()
                data, pod_suggestions, pol_suggestions_from_db = load_from_firestore()
                st.session_state.pod_suggestions = sorted(pod_suggestions)
                mandatory_pols = {"Nhava Sheva", "Rajula", "Pipavav"}
                st.session_state.pol_suggestions = sorted(list(set(pol_suggestions_from_db) | mandatory_pols))
                st.success(f"Data for {vendor} ({month_year}) parsed and stored successfully! Overwritten if previously existed.")
            else:
                st.warning(f"No data extracted for {vendor} ({month_year}). Check file format.")
        except Exception as e:
            st.error(f"Error parsing file: {str(e)}")

    # === Vendors Section ===
    st.subheader("Vendors")
    vendors = ["MSC", "Wan Hai", "Emirates", "ONE MRG", "HMM MRG", "OOCL", "PIL MRG", "ARKAS MRG", "Interasia", "Cosco-Gulf", "Cosco-WCSA & CB", "Cosco-Africa", "Turkon", "Cosco-Fareast", "ZIM MRG"]
    data, _, _ = load_from_firestore()
    num_columns = 5
    num_rows = (len(vendors) + num_columns - 1) // num_columns  # Ceiling division for number of rows

    for row_idx in range(num_rows):
        cols = st.columns(num_columns)
        for col_idx in range(num_columns):
            vendor_idx = row_idx * num_columns + col_idx
            if vendor_idx < len(vendors):
                with cols[col_idx]:
                    vendor = vendors[vendor_idx]
                    record_count = len(data.get(vendor, {}))
                    button_text = f"{vendor} ({record_count} records)"
                    if st.button(button_text, key=f"vendor_{vendor}", help=f"View {vendor} data", use_container_width=True):
                        st.session_state.page = 'vendor'
                        st.session_state.selected_vendor = vendor
                        st.rerun()

    # === Search Vendor Data Section ===
    st.subheader("Search Vendor Data")
    col1, col2, col3, col4, col5 = st.columns([2, 2, 2, 2, 1])

    with col1:
        pol_input = st.selectbox("POL (Type to search)", [""] + st.session_state.pol_suggestions, format_func=lambda x: "" if x == "" else x, help="Start typing to see suggestions")
    with col2:
        # Initialize session state for POD input
        if 'pod_typed_input' not in st.session_state:
            st.session_state.pod_typed_input = ""
        
        # Get current selectbox value
        pod_options = [""] + st.session_state.pod_suggestions
        if st.session_state.pod_typed_input:
            pod_options = [""] + [pod for pod in st.session_state.pod_suggestions if st.session_state.pod_typed_input.lower() in pod.lower()]
            pod_options = sorted(pod_options)
        
        def update_pod_typed_input():
            # Update typed input based on selectbox selection
            selected = st.session_state.pod_select
            st.session_state.pod_typed_input = selected if selected != "" else st.session_state.pod_typed_input

        pod_input = st.selectbox(
            "POD/PORT (Type to search)",
            pod_options,
            format_func=lambda x: "" if x == "" else x,
            key="pod_select",
            help="Start typing to filter port suggestions",
            on_change=update_pod_typed_input
        )

        # Update typed input when user types in the selectbox
        if pod_input != st.session_state.pod_typed_input and pod_input != "":
            st.session_state.pod_typed_input = pod_input
            # Trigger rerun to refresh suggestions
            if pod_input:
                st.rerun()
    with col3:
        carrier_input = st.selectbox("Carrier (Type to search)", [""] + vendors, format_func=lambda x: "" if x == "" else x, help="Start typing to see vendor names")
    with col4:
        st.write("Equipment")
        with st.container():
            cols_equip = st.columns(3)
            equip_20 = cols_equip[0].checkbox("20")
            equip_40std = cols_equip[1].checkbox("40STD")
            equip_40hc = cols_equip[2].checkbox("40HC")
        equipment = [e for e, c in [("20", equip_20), ("40STD", equip_40std), ("40HC", equip_40hc)] if c]
    with col5:
        if st.button("🔍 Search"):
            if not (pol_input or carrier_input):
                st.warning("Please select at least one of POL or Carrier.")
            elif not (pod_input or equipment):
                st.warning("Please enter a POD/PORT or select at least one equipment type.")
            else:
                results = query_firestore(pol_input, pod_input, equipment)
                if carrier_input:
                    results = results[results['Vendor'].str.lower() == carrier_input.lower()] if not results.empty else results
                st.session_state.search_results = results
                if st.session_state.search_results.empty:
                    st.info("No matching records found.")
                    
def vendor_page():
    vendor = st.session_state.selected_vendor
    st.title(f"{vendor} Data")

    if st.button("Back to Main"):
        st.session_state.page = 'main'
        st.session_state.selected_vendor = None
        st.rerun()

    data, _, _ = load_from_firestore()
    if vendor not in data or not data[vendor]:
        st.info(f"No data available for {vendor}.")
        return

    def standardize_currency(value):
        if pd.isna(value):
            return value
        value = str(value).strip()
        # Match formats like "100$", "$100", "100 €", "€200", etc.
        match = re.match(r'^\s*([\$€])?\s*(\d*\.?\d+)\s*([\$€])?\s*$', value)
        if match:
            currency_before, number, currency_after = match.groups()
            if currency_before:
                return f"{currency_before}{number}"
            elif currency_after:
                return f"{currency_after}{number}"
        return value

    for month_year in sorted(data[vendor].keys(), reverse=True):
        with st.expander(f"{month_year[:4]} {month_year[5:]}"):
            df = pd.DataFrame(data[vendor][month_year]["data"])
            info = data[vendor][month_year]["info"]

            if not df.empty:
                text_columns = ['REMARKS', 'ROUTING', 'TRANSIT TIME', 'VALIDITY', 'SERVICE NAME', 'SERVICE', 'EXPIRY DATE']
                df_values = set()
                for col in text_columns:
                    if col in df.columns:
                        df_values.update(df[col].dropna().apply(normalize_text))
                filtered_info = [line for line in info if normalize_text(line) not in df_values and line.strip()]
            else:
                filtered_info = info

            if filtered_info or ('Sheet' in df.columns and df['Sheet'].dropna().unique().size > 0):
                st.subheader("Additional Information")
                if 'Sheet' in df.columns:
                    unique_sheets = df['Sheet'].dropna().unique()
                    if len(unique_sheets) > 0:
                        st.write("**Sheets:**")
                        for sheet in unique_sheets:
                            st.write(f"- {sheet}")
                if filtered_info:
                    st.write("**Details:**")
                    for line in filtered_info:
                        st.write(f"- {line}")

            if not df.empty:
                st.subheader("Data")
                # Ensure POL is displayed consistently and rename POD to PORT for HMM MRG
                if 'POL' in df.columns:
                    df['POL'] = df['POL'].apply(standardize_pol)
                if vendor == "HMM MRG" and 'POD' in df.columns:
                    df = df.rename(columns={'POD': 'PORT'})
                
                # Standardize currency formats in cost-related columns
                cost_columns = ['20', '20 Haz', '40STD', '40HC', '40 Haz', 'IMO SC PER TEU', "40'HRF"]
                for col in cost_columns:
                    if col in df.columns:
                        df[col] = df[col].apply(standardize_currency)
                
                # Standardize column names
                column_mapping = {
                    'Routing': 'ROUTING',
                    'SERVICE NAME': 'SERVICE',
                    'Description': 'REMARKS',
                    'Remarks': 'REMARKS'
                }
                df = df.rename(columns=column_mapping)
                
                # Define the desired column order
                desired_columns = ['POL', 'PORT', '20', '40STD', '40HC', 'ROUTING', 'TRANSIT TIME', 'SERVICE', 'EXPIRY DATE', 'VALIDITY', 'REMARKS']
                
                # Handle related columns (20 Haz after 20, 40 Haz after 40HC)
                final_columns = []
                for col in desired_columns:
                    final_columns.append(col)
                    if col == '20' and '20 Haz' in df.columns:
                        final_columns.append('20 Haz')
                    if col == '40HC' and '40 Haz' in df.columns:
                        final_columns.append('40 Haz')
                
                # Add any additional columns (not in desired_columns or related columns) before REMARKS
                additional_columns = [col for col in df.columns if col not in final_columns]
                if additional_columns:
                    remarks_idx = final_columns.index('REMARKS')
                    final_columns = final_columns[:remarks_idx] + additional_columns + final_columns[remarks_idx:]
                
                # Filter columns that exist in the DataFrame
                final_columns = [col for col in final_columns if col in df.columns]
                
                # Reorder DataFrame columns
                df = df[final_columns]

                st.dataframe(df)

                output_file = f"{vendor}_{month_year}.xlsx"
                excel_buffer = io.BytesIO()
                with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
                    start_row = 0
                    if filtered_info:
                        ws = writer.book.create_sheet(vendor)
                        for i, line in enumerate(filtered_info, start=1):
                            ws.cell(row=i, column=1, value=line)
                        start_row = len(filtered_info) + 1
                    df.to_excel(writer, sheet_name=vendor, startrow=start_row, index=False)
                    if vendor == "HMM MRG":
                        ws = writer.book[vendor]
                        for row in ws.iter_rows(min_row=start_row + 2, min_col=2, max_col=3):
                            for cell in row:
                                if cell.value and isinstance(cell.value, (int, float)):
                                    cell.value = f"${cell.value}"
                                cell.number_format = '@'  # Treat as text to preserve $ prefix
                    if vendor == "ONE MRG" and 'EXPIRY DATE' in df.columns:
                        ws = writer.book[vendor]
                        if start_row == 0:
                            start_row = 6
                            ws.insert_rows(1, amount=6)
                        for i, line in enumerate([l for l in filtered_info if '\n' in l][:6], start=1):
                            ws.cell(row=i, column=1, value=sanitize_text(line))

                excel_buffer.seek(0)

                st.download_button(
                    label=f"Download {month_year} Data",
                    data=excel_buffer,
                    file_name=output_file,
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )

            if st.button(f"Delete {month_year}", key=f"delete_{month_year}"):
                delete_from_firestore(vendor, month_year)
                st.cache_data.clear()
                st.success(f"Deleted {month_year} data for {vendor}.")
                st.rerun()

if st.session_state.page == 'main':
    main_page()
else:
    vendor_page()