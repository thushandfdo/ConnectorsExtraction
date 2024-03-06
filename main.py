import json
import requests
import zipfile
import xml.etree.ElementTree as ET
import re
from datetime import datetime

jsonFile = open('data.json', 'r')
csvFile = open('out.csv', 'w')
logFile = open('log.txt', 'a')
finalJson = open('final.json', 'w')
rankingFile = open('connector-ranks.json', 'r')

ZIP_FILE_PATH = "zip_files/"
connector_icons = {}
connector_ranking = json.load(rankingFile)
next_custom_ranking = len(connector_ranking.keys())


def download_zip(url, file_name, chunk_size=128):
    try:
        if url is None:
            logFile.write("Invalid URL: {}\n".format(file_name))
            return

        r = requests.get(url, stream=True)
        if r.status_code == 200:
            with open(ZIP_FILE_PATH + file_name, 'wb') as fd:
                for chunk in r.iter_content(chunk_size=chunk_size):
                    fd.write(chunk)
        else:
            logFile.write("Download Fail: {} - {}\n".format(file_name, r.status_code))
    except Exception as e:
        logFile.write("Something went wrong: {} \t\t {}\n".format(file_name, e))


def decode_json(record):
    try:
        description = str(record.get('attributes', {}).get('overview_description'))
        description = re.sub(r'\s+', ' ', description)

        record_id = record.get('id')
        name = str(record.get('name')).replace('/', '-').strip()
        version = record.get('version')

        file_name = "{}_{}_{}.zip".format(name, record_id, version)

        return {
            'id': record_id,
            'name': name,
            'version': version,
            'download_url': record.get('attributes', {}).get('overview_downloadlink'),
            'overview_name': record.get('attributes', {}).get('overview_name'),
            'overview_version': record.get('attributes', {}).get('overview_version'),
            'description': description,
            'file_name': file_name
        }
    except Exception as e:
        logFile.write("Decode Fail: {}\n".format(e))
        return None


def read_zip_file(file_path):
    with zipfile.ZipFile(file_path, 'r') as zip_ref:
        config_files = []

        common_config_found = False
        for file_name in zip_ref.namelist():
            if file_name.__contains__('icon/icon-large'):
                ext = file_name.split('.')[-1]
                name = file_path.replace(ZIP_FILE_PATH, "").split('_')[0]
                gif_name = name.replace(' ', '')
                connector_icons[name] = "{}.{}".format(name, ext)
                with open("icons/{}.{}".format(gif_name, ext), "wb") as gif:
                    gif.write(zip_ref.read(file_name))
            elif file_name.__contains__('config/component.xml'):
                config_files.append(zip_ref.read(file_name))
                common_config_found = True
            elif (not common_config_found) and file_name.__contains__('component.xml'):
                config_files.append(zip_ref.read(file_name))

        return config_files


def extract_connectors(file_path):
    try:
        if file_path.endswith(".zip"):
            xml_data = read_zip_file(file_path)

            components = {}
            for xml in xml_data:
                main_component_tag = ET.fromstring(xml)

                for child in main_component_tag[0]:
                    for sub_child in child:
                        if sub_child.tag == 'description':
                            name = str(child.get('name'))
                            description = sub_child.text

                            if description is not None:
                                description = re.sub(r'\s+', ' ', description)

                            components[name] = description

            return components
        else:
            logFile.write("Invalid file type (not zip): {}\n".format(file_path))
            return None
    except Exception as e:
        logFile.write("ZIP Extract Fail: {}\n".format(e))


def download_and_generate_csv(data):
    csvFile.write(
        "#?id?name?version?connectors?download_url?overview_name?overview_version?overview_description?icon\n")

    for index, i in enumerate(data['data']):
        connector = decode_json(i)

        if (connector['download_url'] is None) or connector['name'] == '123ContactForm':
            continue

        # print("Downloading: {} - {}".format(index + 1, connector['name']))
        # download_zip(connector['download_url'], connector['file_name'])

        connectors = extract_connectors(ZIP_FILE_PATH + connector['file_name'])
        str_connectors = ''

        if connectors is not None:
            for key, value in connectors.items():
                if len(str_connectors) > 0:
                    str_connectors += '@'
                str_connectors += "{}-{}".format(key, value)

        csvFile.write("{}?{}?{}?{}?{}?{}?{}?{}?{}?{}\n".format(
            index + 1,
            connector['id'],
            connector['name'],
            connector['version'],
            str_connectors,
            connector['download_url'],
            connector['overview_name'],
            connector['overview_version'],
            connector['description'],
            connector_icons.get(connector['name'], "No Icon")
        ))


def get_rank(connector):
    if connector in connector_ranking:
        return connector_ranking[connector]
    else:
        global next_custom_ranking
        next_custom_ranking += 1
        return next_custom_ranking


def generate_json():
    connectors = []
    with open('out.csv', 'r') as data:
        for line in data:
            if line.__contains__("#?"):
                continue

            sub_json = {}

            line = line.strip().split('?')

            sub_connectors = line[4]
            if sub_connectors is not None and len(sub_connectors) > 0:
                for sub in sub_connectors.split('@'):
                    sub = sub.split('-')
                    sub_json[sub[0]] = sub[1] if len(sub) > 0 else "No Description"

            connector = {
                'name': line[2],
                'version': line[3],
                'rank': get_rank(line[2]),
                'operations': sub_json,
                'download_url': line[5],
                'description': line[8],
                'icon': line[9]
            }
            connectors.append(connector)
    finalJson.write(json.dumps({'data': connectors}, indent=4))


try:
    logFile.write("Start Session: {} ----------------------------- \n\n".format(datetime.now()))

    download_and_generate_csv(json.load(jsonFile))
    generate_json()

    logFile.write("\nEnd Session: {} ------------------------------- \n\n\n".format(datetime.now()))
except Exception as ex:
    logFile.write("\n### Main Fail: {} \n\n\n".format(ex))

jsonFile.close()
csvFile.close()
