#!/usr/local/bin/python

print "+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
print "                               SCANNER PROJECT STATUS"
print "                               version 2.3 (20170215)"
print ""
print "* Written by         : Sander Reukema"
print "* Suggested for by   : Sander W. van der Laan | s.w.vanderlaan-2@umcutrecht.nl"
print "* Last update        : 2017-02-15"
print "* Version            : Status_results_v2.3_20170215"
print ""
print "* Description        : This script will loop over the folders in the RESULTS directory"
print "                       created by the Oddities script and generate a log-file to map"
print "                       the status of each study number"
print ""
print "* NOTE               : Some files present in the OTHER folder might be ignored by the"
print "                       script when the filename is strange, so double check this to "
print "                       be sure"
print ""
print "+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"

import pdb
from collections import OrderedDict

import pandas as pd
import argparse
import os
from time import strftime

DATETODAY = strftime('%Y%m%d')

STATUS_LOG = 'status_log_'+ DATETODAY +'.xlsx'
RESULTS_FOLDER = 'RESULTS'
EXCEL_INSTRUCTIONS = 'AEDB_Valid.xlsx'


def validate_excel_file(filename):
    """validate argument for --excel and --output"""
    excel_formats = ['xlsx', 'xlsm', 'xlsb', 'xltx', 'xltm', 'xls', 'xlt',
                     'xls', 'xml', 'xlam', 'xla', 'xlw', 'csv']

    if filename.split('.')[-1] in excel_formats:
        return filename
    else:
        raise ValueError()


def parse_args():
    parser = argparse.ArgumentParser(description='Make revision of files '
                                                 'created by Oddities.py')

    parser.add_argument(
        '--excel',
        help="File name of the excel file with the instruction for the "
             "script. File extension should be in "
             "'xlsx', 'xlsm', 'xlsb', 'xltx', 'xltm', 'xls', 'xlt',"
             "'xls', 'xml', 'xlam', 'xla', 'xlw', 'csv'",
        type=validate_excel_file,
        default=EXCEL_INSTRUCTIONS
    )

    parser.add_argument(
        '--output',
        help="File name of the excel log file "
             "which would be created by the script. "
             "File extension should be in 'xlsx', 'xlsm', 'xlsb', 'xltx', 'xltm', 'xls', 'xlt',"
             "'xls', 'xml', 'xlam', 'xla', 'xlw', 'csv'",
        type=validate_excel_file,
        default=STATUS_LOG
    )

    parser.add_argument(
        '--file-folder',
        help="Name of the folder which contains RESULTS. This folders is "
             "created by the Oddities.py script",
        default=RESULTS_FOLDER
    )

    return parser.parse_args()


def search_sN_in_folders(study_number):
    found_in_dir, found_without_dir_payload = False, []
    matched_files_in_dir, matched_files_in_wo_dir = dict(), dict()
    duplicate_folders = []

    for dirpath, dirnames, filenames in os.walk(parse_args().file_folder):
        for dir in dirnames:
            if dir.endswith(study_number + '.') and dir not in matched_files_in_dir:
                found_in_dir = True
                dir_path = os.path.join(dirpath, dir)

                if len(duplicate_folders) > 0:
                    unique = True
                    for item in duplicate_folders:
                        if item == dir_path.split('/')[1]:
                            unique = False
                    if unique:
                        duplicate_folders.append(dir_path.split('/')[-2])
                else:
                    duplicate_folders.append(dir_path.split('/')[-2])

                matched_files_in_dir[dir_path] = os.listdir(dir_path)

        # in case if file not in folder
        files_wo_folder = [file for file in filenames
                           if file.startswith(study_number + '.')
                           and file.endswith('.TIF')]
        if files_wo_folder:
            dir_path = os.path.basename(os.path.normpath(dirpath))
            matched_files_in_wo_dir[dir_path + '/just_stub'] = files_wo_folder

            found_without_dir_payload = 1, matched_files_in_wo_dir, [dir_path]

    is_scanned = 1 if matched_files_in_dir else 0

    if not found_in_dir and found_without_dir_payload:
        return found_without_dir_payload
    else:
        if len(found_without_dir_payload) == 3 \
                and 'NEW' in found_without_dir_payload[2]:
            return is_scanned, matched_files_in_dir, ['NEW']
        else:
            return is_scanned, matched_files_in_dir, duplicate_folders


def is_rescan(folder, ):
    rescan = 0
    if folder in ['BAD_SLIDES', 'SCAN_ERROR', 'NO_TIF']:
        rescan = 1

    return rescan


def is_duplicates(folder):
    duplicates = 0
    if folder.endswith('DUPLICATES'):
        duplicates = 1

    return duplicates


def is_weirdo(folder):
    weirdo = 0
    if folder in 'OTHER':
        weirdo = 1

    return weirdo


def is_rename(folder):
    rename = 0
    if folder in ['CHECK_T_NUMBER', 'NO_MASK_CHECK_T_NUMBER', 'NEW']:
        rename = 1

    return rename


def is_make_mask(folder):
    mask = 0
    if folder in 'NO_MASK':
        mask = 1

    return mask


def is_check_mask(folder):
    mask = 0
    if folder in 'OKAY':
        mask = 1

    return mask


def choose_dir_by_priority(dirs):
    keys = {
        'BAD_SLIDES': 1, 'SCAN_ERROR': 1, 'NO_TIF': 1,
        'CHECK_T_NUMBER_DUPLICATES': 2, 'NO_MASK_CHECK_T_NUMBER_DUPLICATES': 2,
        'NO_MASK_DUPLICATES': 2, 'OKAY_DUPLICATES': 2,
        'OTHER': 3, 'CHECK_T_NUMBER': 4, 'NEW': 4,
        'NO_MASK_CHECK_T_NUMBER': 4, 'NO_MASK': 5, 'OKAY': 6, RESULTS_FOLDER: 7
    }
    mapped_dirs = {k: v for k, v in keys.items() if k in dirs}

    return max(mapped_dirs, key=mapped_dirs.get) if len(mapped_dirs) \
        else 'RESULTS'


def process_row(index, study_number, valid):
    """Responsible for processing single row from excel"""

    # search if file with specified study number is presented in the RESULTS
    # returns a status and filenames (if exists)
    is_scanned, scanned_files, duplicates = search_sN_in_folders(study_number)

    if is_scanned:
        for folder, files in scanned_files.iteritems():
            print 'Found file which corresponds to STUDY NUMBER: %s' % study_number

            if len(duplicates) > 1:
                folders = ",".join(duplicates)
            elif 'NEW' in duplicates:
                folders = 'NEW'
            else:
                folders = os.path.dirname(folder).split('/')[-1]

            if type(duplicates) == list:
                folder = choose_dir_by_priority(duplicates)
            else:
                # because folders only ony one folder
                folder = folders

            # functions below are generating results for every column
            # in future log file
            rescan = is_rescan(folder)
            uniquefy = is_duplicates(folder)
            weirdo = is_weirdo(folder)
            rename = is_rename(folder)
            mask_making = is_make_mask(folder)
            mask_checking = is_check_mask(folder)
            if folder == RESULTS_FOLDER:
                finished = 1
                folders = 'ROOT'
            else:
                finished = 0

            # calculating "scanning" column
            scanning = not (
                rescan or uniquefy or weirdo or rename or
                mask_making or mask_checking
            )

            if finished:
                scanning, rescan, uniquefy, \
                weirdo, rename, mask_checking, mask_making = 0, 0, 0, 0, 0, 0, 0

            return {
                'INDEX': index, 'STUDY_TYPENUMBER': study_number,
                'Valid_UPID': valid,
                'Scanning': int(scanning), 'Rescan': rescan,
                'Uniquefy': uniquefy,
                "Check weirdo's": weirdo, 'Rename': rename,
                'Mask making': mask_making, 'Mask checking': mask_checking,
                'Finished': finished, 'FOLDER': folders
            }
    else:
        return {
            'INDEX': index, 'STUDY_TYPENUMBER': study_number,
            'Valid_UPID': valid, 'Scanning': 1, 'Rescan': 0,
            'Uniquefy': 0, "Check weirdo's": 0, 'Rename': 0,
            'Mask making': 0, 'Mask checking': 0, 'Finished': 0, 'FOLDER': 0
        }


def iterate_excel():
    """Generator that iterates every row in the AEDB_Valid.xlsx.
    Returns a dict with result of processing row from excel file"""

    for line in pd.read_excel(parse_args().excel).iterrows():
        try:
            print 'Processing %s...' % line[1].STUDY_TYPENUMBER

            # Getting a dict with results from one row
            row_dict = process_row(
                line[1].Index, line[1].STUDY_TYPENUMBER, line[1].Valid_UPID
            )

            if row_dict:
                yield row_dict
        except Exception as e:
            print('Error while processing row. Skipping it...'
                  '{0}'.format(e))


def save_to_excel(log_dataframe):
    """Storing dataframe with log into new excel file"""

    writer = pd.ExcelWriter(parse_args().output)
    log_dataframe = log_dataframe.reset_index().drop('index', 1)
    log_dataframe.to_excel(
        writer, sheet_name='result', index=False
    )
    writer.save()


def add_statistic(data):
    valid_stat, invalid_stat = OrderedDict(), OrderedDict()

    empty = {
        'INDEX': '', 'STUDY_TYPENUMBER': '', 'Valid_UPID': '', 'Scanning': '',
        'Rescan': '', 'Uniquefy': '', "Check weirdo's": '', 'Rename': '',
        'Mask making': '', 'Mask checking': ''
    }

    valid_stat['INDEX'] = ''
    valid_stat['STUDY_TYPENUMBER'] = 'Totals'
    valid_stat['Valid_UPID'] = 'yes'
    valid_stat['Scanning'] = 0
    valid_stat['Rescan'] = 0
    valid_stat['Uniquefy'] = 0
    valid_stat["Check weirdo's"] = 0
    valid_stat['Rename'] = 0
    valid_stat['Mask making'] = 0
    valid_stat['Mask checking'] = 0
    valid_stat['Finished'] = 0


    invalid_stat['INDEX'] = ''
    invalid_stat['STUDY_TYPENUMBER'] = ''
    invalid_stat['Valid_UPID'] = 'no'
    invalid_stat['Scanning'] = 0
    invalid_stat['Rescan'] = 0
    invalid_stat['Uniquefy'] = 0
    invalid_stat["Check weirdo's"] = 0
    invalid_stat['Rename'] = 0
    invalid_stat['Mask making'] = 0
    invalid_stat['Mask checking'] = 0
    invalid_stat['Finished'] = 0

    for row in data.iterrows():
        if row[1]['Valid_UPID'] == 'yes':
            if row[1]['Scanning']:
                valid_stat['Scanning'] = valid_stat.get('Scanning', 0) + 1
            if row[1]['Rescan']:
                valid_stat['Rescan'] = valid_stat.get('Rescan', 0) + 1
            if row[1]['Uniquefy']:
                valid_stat['Uniquefy'] = valid_stat.get('Uniquefy', 0) + 1
            if row[1]["Check weirdo's"]:
                valid_stat["Check weirdo's"] = valid_stat.get("Check weirdo's", 0) + 1
            if row[1]['Rename']:
                valid_stat['Rename'] = valid_stat.get('Rename', 0) + 1
            if row[1]['Mask making']:
                valid_stat['Mask making'] = valid_stat.get('Mask making', 0) + 1
            if row[1]['Mask checking']:
                valid_stat['Mask checking'] = valid_stat.get('Mask checking', 0) + 1
            if row[1]['Finished']:
                valid_stat['Finished'] = valid_stat.get('Finished', 0) + 1
        elif row[1]['Valid_UPID'] == 'no':
            if row[1]['Scanning']:
                invalid_stat['Scanning'] = invalid_stat.get('Scanning', 0) + 1
            if row[1]['Rescan']:
                invalid_stat['Rescan'] = invalid_stat.get('Rescan', 0) + 1
            if row[1]['Uniquefy']:
                invalid_stat['Uniquefy'] = invalid_stat.get('Uniquefy', 0) + 1
            if row[1]["Check weirdo's"]:
                invalid_stat["Check weirdo's"] = invalid_stat.get(
                    "Check weirdo's", 0) + 1
            if row[1]['Rename']:
                invalid_stat['Rename'] = invalid_stat.get('Rename', 0) + 1
            if row[1]['Mask making']:
                invalid_stat['Mask making'] = invalid_stat.get('Mask making', 0) + 1
            if row[1]['Mask checking']:
                invalid_stat['Mask checking'] = invalid_stat.get('Mask checking', 0) + 1
            if row[1]['Finished']:
                invalid_stat['Finished'] = invalid_stat.get('Finished', 0) + 1

    valid_stat['FOLDER'] = 0
    invalid_stat['FOLDER'] = 0

    data = data.append(empty, ignore_index=True)
    data = data.append(valid_stat, ignore_index=True)
    data = data.append(invalid_stat, ignore_index=True)

    return data


def mk_log_array():
    """Main function, responsible for iterating excel,
    creating DataFrame and storing it as new excel file"""

    result_dataframe = pd.DataFrame()

    for row in iterate_excel():
        result_dataframe = result_dataframe.append(row, ignore_index=True)[
            ['INDEX', 'STUDY_TYPENUMBER', 'Valid_UPID', 'Scanning',
             'Rescan', 'Uniquefy', "Check weirdo's", 'Rename',
             'Mask making', 'Mask checking', 'Finished', 'FOLDER']
        ]

    result_dataframe = add_statistic(result_dataframe)
    save_to_excel(result_dataframe)


if __name__ == '__main__':
    print '\n \t>Starting file processing\n'

    mk_log_array()

    print '\n \t>Processing is finished!\n'

print ""
THISYEAR = strftime('%Y')
print "+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
print "+ The MIT License (MIT)                                                                                 +"
print "+ Copyright (c) 2015-" + THISYEAR + " Sander W. van der Laan                                                        +"
print "+                                                                                                       +"
print "+ Permission is hereby granted, free of charge, to any person obtaining a copy of this software and     +"
print "+ associated documentation files (the \"Software\"), to deal in the Software without restriction,         +"
print "+ including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, +"
print "+ and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, +"
print "+ subject to the following conditions:                                                                  +"
print "+                                                                                                       +"
print "+ The above copyright notice and this permission notice shall be included in all copies or substantial  +"
print "+ portions of the Software.                                                                             +"
print "+                                                                                                       +"
print "+ THE SOFTWARE IS PROVIDED \"AS IS\", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT     +"
print "+ NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND                +"
print "+ NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES  +"
print "+ OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN   +"
print "+ CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.                            +"
print "+                                                                                                       +"
print "+ Reference: http://opensource.org.                                                                     +"
print "+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"

