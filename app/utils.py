import dropbox
from dropbox.files import WriteMode
import os

dbx = dropbox.Dropbox(os.environ.get('DROPBOX_TOKEN'))

def pretty_time(seconds):
    """Convert a number of seconds to hours minutes seconds"""
    seconds = abs(int(seconds))
    days, seconds = divmod(seconds, 86400)
    hours, seconds = divmod(seconds, 3600)
    minutes, seconds = divmod(seconds, 60)
    if days > 0:
        return '%dd %dh %dm %ds' % (days, hours, minutes, seconds)
    elif hours > 0:
        return '%dh %dm %ds' % (hours, minutes, seconds)
    elif minutes > 0:
        return '%dm %ds' % (minutes, seconds)
    else:
        return '%ds' % (seconds)

def upload_to_dropbox(file_name, csv_output):
    """Upload a file to dropbox"""
    global dbx
    file_folder = os.environ.get('UPLOAD_FOLDER_NAME')
    file_path = f'/{file_folder}/{file_name}'
    try:
        dbx.files_upload(csv_output, file_path, mode=WriteMode('overwrite'))
    except Exception as err:
        print(f'Failed upload to dropbox because {str(err)}')