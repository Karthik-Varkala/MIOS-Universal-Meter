import os
import csv
import xml.etree.ElementTree as ET
from pathlib import Path
from fastapi import HTTPException

def get_cdf_files(directory_path: str):
    if not os.path.isdir(directory_path):
        raise HTTPException(status_code=400, detail=f"Directory not found: {directory_path}")
    files = [f for f in os.listdir(directory_path) if f.lower().endswith('.cdf')]
    if not files:
        raise HTTPException(status_code=404, detail="No .cdf files found.")
    return files

def get_meter_no(root):
    d1 = root.find('.//D1')
    if d1 is not None:
        g1 = d1.find('G1')
        if g1 is not None and g1.text:
            return g1.text
    return "Unknown"

def extract_instantaneous(root, meter_no):
    data = []
    d2 = root.find('.//D2')
    if d2 is not None:
        for param in d2.findall('INSTPARAM'):
            data.append({
                "MeterNo": meter_no,
                "Code": param.get('CODE'),
                "Value": param.get('VALUE'),
                "Unit": param.get('UNIT')
            })
    return data

def extract_load_profile(root, meter_no):
    data = []
    d4 = root.find('.//D4')
    if d4 is not None:
        for day_profile in d4.findall('DAYPROFILE'):
            date = day_profile.get('DATE')
            for ip in day_profile.findall('IP'):
                row = {'MeterNo': meter_no, 'Date': date, 'Interval': ip.get('INTERVAL')}
                for param in ip.findall('PARAMETER'):
                    row[param.get('PARAMCODE')] = param.get('VALUE')
                data.append(row)
    return data

def extract_billing(root, meter_no):
    data = []
    d3 = root.find('.//D3')
    if d3 is not None:
        for sub in d3:
            dt = sub.get('DATETIME')
            for b_tag in sub:
                if b_tag.tag == 'B3':
                    data.append({
                        "MeterNo": meter_no, "Section": sub.tag,
                        "DateTime": dt, "Code": b_tag.get('PARAMCODE'),
                        "Value": b_tag.get('VALUE'), "Unit": b_tag.get('UNIT')
                    })
    return data

def save_csv(data, file_path, headers, is_dict_writer=False):
    if not data:
        return
    with open(file_path, 'w', newline='') as f:
        if is_dict_writer:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            writer.writerows(data)
        else:
            writer = csv.writer(f)
            writer.writerow(headers)
            for row in data:
                writer.writerow([row[h] for h in headers])