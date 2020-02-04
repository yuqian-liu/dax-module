from dax import XnatUtils, SessionModule
from dax.version import VERSION as __version__
from dax.git_revision import git_revision as __git_revision__
import os
import csv
import shutil
import yaml
import requests
from datetime import datetime
from shutil import copyfile
import tempfile
from os.path import expanduser
import glob
import logging
from lxml import etree

DEFAULT_MODULE_NAME = 'Module_baxter_redcap_sync'
DEFAULT_TEXT_REPORT = 'ERROR/WARNING for Module_baxter_redcap:\n'
DEFAULT_MRI_FIELDS = ['record_id', 'script_version', 'last_update_module']
REDCAP_FLAG = 'UPLOADED_TO_REDCAP2'
REDCAP_FLAG1 = 'abc'
REDCAP_FILE = os.path.join(expanduser("~"), '.redcap.yaml')
DEFAULT_TMP_PATH = os.path.join('/tmp', DEFAULT_MODULE_NAME)

DEFAULT_DATA_DICTIONARY_TEMPLATE = 'field_name,form_name,section_header,field_type,field_label,\
select_choices_or_calculations,field_note,text_validation_type_or_show_slider_number,\
text_validation_min,text_validation_max,identifier,branching_logic,required_field,\
custom_alignment,question_number,matrix_group_name,matrix_ranking,field_annotation\n\
record_id,quality_control,,text,"record_id",,,,,,,,,,,,,\n\
assessor_label,quality_control,,text,"assessor_label",,,,,,,,,,,,,\n\
project,quality_control,,text,"project",,,,,,,,,,,,,\n\
subject,quality_control,,text,"subject",,,,,,,,,,,,,\n\
experiment,quality_control,,text,"experiment",,,,,,,,,,,,,\n\
proctype,quality_control,,text,"proc_type",,,,,,,,,,,,,\n\
proc_version,quality_control,,text,"proc version",,,,,,,,,,,,,\n\
proc_date,quality_control,,text,"proc_date",,,,,,,,,,,,,\n\
dax_version_hash,quality_control,,text,"dax_version_hash",,,,,,,,,,,,,\n\
dax_version,quality_control,,text,"dax_version",,,,,,,,,,,,,\n\
id,quality_control,,text,"ID",,,,,,,,,,,,,\n'

LOGGER = logging.getLogger('dax')

class Module_baxter_redcap_sync(SessionModule):
    def __init__(self,
                 directory='',
                 mod_name=DEFAULT_MODULE_NAME,
                 text_report=DEFAULT_TEXT_REPORT,
                 resources='',
                 proctypes=''):
        super(Module_baxter_redcap_sync, self).__init__(
            mod_name, '/tmp/Module_baxter_redcap_sync', None, text_report=text_report)

        self.resourcess = [r.split(',') for r in resources.split(';')]
        self.proctypes = proctypes.split(';')
        self.need_to_run_assessors = list()
        self.tmp_path = tempfile.mkdtemp()
        self.xnat = None

    def prerun(self, settings_filename=''):
        self.xnat = XnatUtils.get_interface()

    def afterrun(self, xnat, project):
        pass

    def needs_run(self, csess, xnat):
        self.need_to_run_assessors = csess.assessors()
        return True

    def run(self, sess_info, sess_obj):
        flagfile = os.path.join('/tmp', '%s_%s' % (sess_obj.label(), 'LOCK'))
        # skip if already running
        success = self.lock_flagfile(flagfile)
        if not success:
            LOGGER.info('failed to get lock. Already running.')
            return 'Session:'+ sess_obj.label() +' is already running.'
        stdout = ""
        info = dict()
        info.update(project=sess_info['project_label'])
        info.update(subject=sess_info['subject_label'])
        info.update(session=sess_obj.label())
        info.update(dax_version=__version__)
        info.update(dax_version_hash=__git_revision__)

        # Cycle over proctypes
        for idx, proctype in enumerate(self.proctypes):
            # Get corresponding resources and project name
            resources = self.resourcess[idx]
            redcap_project = info['project'] + "-" + proctype + "-" + resources[0]
            for assessor in self.need_to_run_assessors:
                ass_info = assessor.info()
                if ass_info.get('proctype') == proctype and ass_info.get('procstatus') == 'COMPLETE':
                    info.update(proc_date=ass_info.get('jobstartdate'))
                    info.update(proc_version=ass_info.get('version'))
                    info.update(assessor_label=ass_info.get('assessor_label'))
                    info.update(proc_date=ass_info.get('jobstartdate'))
                    info.update(id=ass_info.get('ID'))
                    info.update(proctype=proctype)
                    inputs = assessor.get_inputs()
                    inputs = {y.decode('ascii'): inputs.get(y).decode('ascii') for y in inputs.keys()}
                    stdout += self.redcap_sync(redcap_project, info, inputs, resources)
        shutil.rmtree(self.tmp_path)
        self.unlock_flagfile(flagfile)
        return(stdout)

    def redcap_sync(self, redcap_project, info, inputs, resources):
        stdout = ''
        api_url = get_api_url(REDCAP_FILE)
        api_key = get_project_api_key(REDCAP_FILE, redcap_project)
        project = info['project']
        subject = info['subject']
        session = info['session']
        assessor_label = info['assessor_label']
        # Find resources which match and haven't been uploaded before, then download them,
        # find csvs, append to list and mark resource as uploaded
        csv_paths = []
        resources_uploaded = []
        payload = {'format': 'xml'}
        response = self.xnat.get('data/projects/' + project +
                            '/subjects/' + subject +
                            '/experiments/' + session +
                            '/assessors/' + assessor_label,
                            params=payload)
        # unlock and return if query failed
        if response.status_code != 200:
            msg = 'Session:'+ session +' failed to get assessor ' + assessor_label
            LOGGER.error(msg)
            return msg

        assessor_root = etree.fromstring(response.content)
        for idx, out_element in enumerate(assessor_root.find('xnat:out', assessor_root.nsmap)):
            resource = out_element.get('label')
            if resource in resources:
                # Check to make sure resource hasn't already been uploaded
                note_element = out_element.find('xnat:note', assessor_root.nsmap)
                if note_element is not None and note_element.text == REDCAP_FLAG1:
                    msg = 'Session:'+ session +', proc: ' + info['proctype'] + ' already uploaded to REDCAP.\n'
                    LOGGER.debug(msg)
                    stdout += msg
                    continue

                # Download resources
                res_obj = XnatUtils.select_obj(self.xnat, project, subject, session,
                                               assessor_id=assessor_label, resource=resource)
                try:
                    XnatUtils.download_file_from_obj(self.tmp_path, res_obj)
                except Exception as e:
                    msg = 'Session:' + session + ', proc:' + info['proctype']\
                          + ' failed to download resource.\n'
                    LOGGER.error(e)
                    return msg

                # Find csv files
                for csv_path in glob.iglob(os.path.join(self.tmp_path, '*.csv')):
                    csv_paths += [csv_path]
                # Mark this resource as uploaded
                resources_uploaded += [resource]
                LOGGER.info('Found the following csv(s): ' + str(csv_paths))

                # syncs csvs to redcap
                record_header = ['record_id','assessor_label','project','subject','experiment','proctype',
                                 'proc_version','proc_date','dax_version_hash',
                                 'dax_version','id','quality_control_complete']
                record_header_dict = {'record_id': 0,'assessor_label': 1,'project': 2,'subject': 3,'experiment': 4,
                                      'proctype': 5,'proc_version': 6,'proc_date': 7,'dax_version_hash': 8,
                                      'dax_version': 9,'id': 10,'quality_control_complete': 11}
                data_dictionary = DEFAULT_DATA_DICTIONARY_TEMPLATE
                for key in inputs:
                    record_header.append('input_' + key)
                    record_header.append('input_' + key + '_label')
                    record_header_dict['input_' + key] = len(record_header_dict)
                    record_header_dict['input_' + key + '_label'] = len(record_header_dict)
                    data_dictionary += ','.join(['input_' + key, 'quality_control', '', 'text', '"' + 'input_' + key + '"',
                              '', '', '', '', '', '', '',
                              '', '', '', '', '', '']) + '\n'
                    data_dictionary += ','.join(
                        ['input_' + key + '_label', 'quality_control', '', 'text', '"' + 'input_' + key + 'label' '"',
                         '', '', '', '', '', '', '',
                         '', '', '', '', '', '']) + '\n'

                try:
                    for csv_path in csv_paths:
                        with open(csv_path, newline='') as file:
                            reader = csv.reader(file)
                            header = next(reader)
                            instrument = os.path.splitext(os.path.basename(csv_path))[0]
                            for var in header:
                                var = var.strip().lower()
                                record_header += [var]
                                record_header_dict[var] = len(record_header_dict)
                                data_dictionary += ','.join([var, instrument, '', 'text', '"' + var + '"',
                                                             '', '', '', '', '', '', '',
                                                             '', '', '', '', '', '']) + '\n'
                            record_header += [instrument + '_complete']
                            record_header_dict[instrument + '_complete'] = len(record_header_dict)

                    # Set data dictionary only if project has no records
                    records = get_records(api_url, api_key)
                    num_records = len(records.strip().split('\n')) - 1
                    #if num_records == 0:
                        #set_data_dictionary(api_url, api_key, data_dictionary)

                    record = [''] * len(record_header)
                    record[record_header_dict['assessor_label']] = assessor_label
                    record[record_header_dict['project']] = project
                    record[record_header_dict['subject']] = subject
                    record[record_header_dict['experiment']] = session
                    record[record_header_dict['proc_version']] = info['proc_version']
                    record[record_header_dict['proc_date']] = info['proc_date']
                    record[record_header_dict['dax_version_hash']] = info['dax_version_hash']
                    record[record_header_dict['dax_version']] = info['dax_version']
                    record[record_header_dict['proctype']] = info['proctype']
                    record[record_header_dict['id']] = info['id']
                    record[record_header_dict['quality_control_complete']] = '1'
                    for key in inputs:
                        record[record_header_dict['input_' + key]] = inputs[key]
                        record[record_header_dict['input_' + key + '_label']] = inputs[key].split('/')[-1]
                    record_id = 0
                    records_data = ','.join(record_header) + '\n'
                    num_csv = len(csv_paths)
                    for csv_path in csv_paths:
                        with open(csv_path, newline='') as file:
                            reader = csv.reader(file)
                            header = next(reader)
                            instrument = os.path.splitext(os.path.basename(csv_path))[0]
                            for line in reader:
                                record[record_header_dict['record_id']] = str(record_id)
                                record_id += 1
                                # Set values in csv
                                for idx, var in enumerate(line):
                                    record[record_header_dict[header[idx].strip().lower()]] = var.strip()
                                # Do the "complete" thing
                                record[record_header_dict[instrument + '_complete']] = '1'
                                if num_csv == 1:
                                    records_data += ','.join(record) + '\n'
                    if num_csv > 1:
                        records_data += ','.join(record) + '\n'

                    # Set flag to let process know this resource has been uploaded to redcap
                    for idx, out_element in enumerate(assessor_root.find('xnat:out', assessor_root.nsmap)):
                        resource = out_element.get('label')
                        if resource in resources_uploaded:
                            payload = {'xsiType': 'proc:genProcData',
                                       'proc:genProcData/out/file[' + str(idx) + ']/note': 'will resend'}
                            response = self.xnat.put('data/projects/' + project +
                                                '/subjects/' + subject +
                                                '/experiments/' + session +
                                                '/assessors/' + assessor_label,
                                                params=payload)
                            if response.status_code == 200:
                                # insert records to redcap
                                #set_records(api_url, api_key, records_data)
                                print('Marking ' + resource + ' as uploaded.')
                                msg = 'Session:' + session + ', proc:' + info['proctype']\
                                      + ' success uploaded to redcap\n'
                                LOGGER.info(msg)
                                return msg
                            else:
                                msg = 'Session:' + session + ', proc:' + info['proctype']\
                                      + ' failed to update the resource note.\n'
                                LOGGER.error(msg)
                                return msg
                except Exception as e:
                    LOGGER.error(e)
                    return e
        return stdout

    @staticmethod
    def lock_flagfile(lock_file):
        """
        Create the flagfile to lock the process
        :param lock_file: flag file use to lock the process
        :return: True if the file didn't exist, False otherwise
        """
        if os.path.exists(lock_file):
            return False
        else:
            open(lock_file, 'w').close()
            return True

    @staticmethod
    def unlock_flagfile(lock_file):
        """
        Remove the flagfile to unlock the process

        :param lock_file: flag file use to lock the process
        :return: None
        """
        if os.path.exists(lock_file):
            os.remove(lock_file)
def check_dir(dir_path):
    try:
        os.makedirs(dir_path)
    except OSError:
        if not os.path.isdir(dir_path):
            raise
####################################################################################
#                                       REDCap Utils                               #
####################################################################################

def create_project(api_url, api_key, project):
    """ creates a project with a super api token """
    payload = {'token': api_key,
               'content': 'project',
               'format': 'csv',
               'data': 'project_title,purpose\n"' + project + '",0'}

    response = requests.post(api_url, data=payload)

    if response.status_code == 200:
        return response.content.decode(response.encoding)
    else:
        raise RuntimeError('Could not create project. Response content: ' +
                           response.content.decode(response.encoding))

def get_api_url(redcap_file):
    """ This returns api url """

    # Make sure redcap file exists
    if not os.path.isfile(redcap_file):
        raise RuntimeError('redcap file: "' + redcap_file + '" could not be found.')

    # Read inputs yaml as dictionary
    with open(redcap_file, 'rt') as file:
        redcap_data = yaml.load(file, yaml.SafeLoader)

    # Get super api token first
    if 'api_url' in redcap_data:
        return redcap_data['api_url']
    else:
        raise RuntimeError('redcap file: "' + redcap_file + '" is not formatted correctly. ' +
                           '"api_url" attribute could not be found.')

def check_project_api_key(redcap_file, project):
    """ Given a "redcap yaml file" and project this will return api key if it exists """

    # Make sure redcap file exists
    if not os.path.isfile(redcap_file):
        raise RuntimeError('redcap file: "' + redcap_file + '" could not be found.')

    # Read inputs yaml as dictionary
    with open(redcap_file, 'rt') as file:
        redcap_data = yaml.load(file, yaml.SafeLoader)

    # Now find api key
    api_key = None
    if 'projects' in redcap_data:
        project_dicts = redcap_data['projects']
        if project_dicts is not None:
            for project_dict in project_dicts:
                if 'key' in project_dict and 'name' in project_dict:
                    if project_dict['name'].strip() == project:
                        print('Project: "' + project + '" found!')

                        # Store api key
                        api_key = project_dict['key'].strip()
                else:
                    raise RuntimeError('project: ' + str(project_dict) + ' was not formatted ' +
                                       'correct. It should contain a "key" and "name" attribute.')
    else:
        raise RuntimeError('redcap file: "' + redcap_file + '" is not formatted correctly. ' +
                           '"projects" attribute could not be found.')

    if not api_key:
        print('Project: "' + project + '" not found...')

    return api_key

def get_project_api_key(redcap_file, project):
    """ This returns a project api token; if project doesnt exist it will get created """

    # Make sure redcap file exists
    if not os.path.isfile(redcap_file):
        raise RuntimeError('redcap file: "' + redcap_file + '" could not be found.')

    # Read inputs yaml as dictionary
    with open(redcap_file, 'rt') as file:
        redcap_data = yaml.load(file, yaml.SafeLoader)

    # Check first if project already exists
    api_key = check_project_api_key(redcap_file, project)
    if not api_key:
        # Project doesn't exist; create the project, append it to redcap_file, then return API key
        print('Creating new redcap project: "' + project + '"')

        # Get super api token first
        if 'api_url' in redcap_data and 'super_api_key' in redcap_data:
            api_url = redcap_data['api_url']
            super_api_key = redcap_data['super_api_key']

            # Create project
            api_key = create_project(api_url, super_api_key, project)

            # Append to redcap file
            if redcap_data['projects'] is not None:
                redcap_data['projects'].append({'key': api_key, 'name': project})
            else:
                redcap_data['projects'] = [{'key': api_key, 'name': project}]

            # Create backup of old redcap file
            redcap_backup_file = os.path.splitext(redcap_file)[0] + '_' + \
                                 datetime.now().strftime('%Y_%m_%d_%H_%M_%S_%f') + \
                                 '.yaml'
            copyfile(redcap_file,redcap_backup_file)
            print('Created backup of redcap file: "' + redcap_backup_file + '"')

            # Now save redcap file with new project
            print('Saving new redcap file ...')
            with open(redcap_file, 'w') as file:
                yaml.safe_dump(redcap_data, file, default_flow_style=False)
        else:
            raise RuntimeError('redcap file: "' + redcap_file + '" is not formatted correctly. ' +
                               '"super_api_key" attribute could not be found.')

    return api_key

def get_records(api_url, api_key):
    """ returns records of a redcap project given an api url and api key """
    payload = {'token': api_key,
               'content': 'record',
               'format': 'csv',
               'type': 'flat'}

    response = requests.post(api_url, data=payload)

    if response.status_code == 200:
        return response.content.decode(response.encoding)
    else:
        raise RuntimeError('Could not set records. Response content: ' +
                           response.content.decode(response.encoding))

def set_records(api_url, api_key, data):
    """ set records of a redcap project given an api url, api key, and data """
    payload = {'token': api_key,
               'content': 'record',
               'format': 'csv',
               'type': 'flat',
               'overwriteBehavior': 'overwrite',
               'forceAutoNumber': 'true',
               'returnContent': 'auto_ids',
               'data': data}

    # If forceAutoNumber is set to true, redcap will automatically assign a record number.
    # However, record numbers during the import are still required to associate rows to the same
    # records

    response = requests.post(api_url, data=payload)

    if response.status_code == 200:
        return response.content.decode(response.encoding)
    else:
        raise RuntimeError('Could not set records. Response content: ' +
                           response.content.decode(response.encoding))

def set_data_dictionary(api_url, api_key, data_dictionary):
    """ sets data dictionary of a redcap project given an api url and api key """
    payload = {'token': api_key,
               'content': 'metadata',
               'format': 'csv',
               'data': data_dictionary}

    response = requests.post(api_url, data=payload)

    if response.status_code == 200:
        return response.content
    else:
        raise RuntimeError('Could not set data dictionary. Response content: ' +
                           response.content.decode(response.encoding))
if __name__ == '__main__':
    d = Module_baxter_redcap_sync(
                 directory='/Users/yuqian',
                 mod_name=DEFAULT_MODULE_NAME,
                 text_report=DEFAULT_TEXT_REPORT,
                 resources='STATS',
                 proctypes='naleg-roi_v1')
    xnat = XnatUtils.get_interface("http://xnat.vanderbilt.edu:8080/xnat","masispider","xnatisawesome!!")
    csess = XnatUtils.CachedImageSession(xnat, 'IKIZLER', '133511', '133511')
    d.prerun()
    d.needs_run(csess,xnat)
    d.run(csess.info(), csess.full_object())

