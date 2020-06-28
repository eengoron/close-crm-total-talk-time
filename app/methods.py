import gevent
import gevent.monkey
from gevent.pool import Pool
gevent.monkey.patch_all()
pool = Pool(7)

from closeio_api import Client as CloseIO_API, APIError
import os
import math
from operator import itemgetter
import csv
import io
from datetime import datetime

from app.utils import pretty_time, upload_to_dropbox


leads = []
calls_per_lead = []
api = None
user_ids_to_names = {}

def _get_leads_slice(slice_num):
    """Get all leads for a particular slice number"""
    print("Getting lead slice %s of %s..." % (slice_num, total_slices))
    has_more = True
    offset = 0 
    while has_more:
        resp = api.get('lead', params={ '_skip': offset, 'query': 'sort:created slice:%s/%s' % (slice_num, total_slices), '_fields':'id,display_name' })
        for lead in resp['data']:
            leads.append(lead)
        offset += len(resp['data'])
        has_more = resp['has_more']

def _get_all_leads():
    """Calculate the slice number and get all leads using gevent"""
    global total_slices
    total_leads = api.get('lead', params={ '_limit': 0, 'query': 'sort:created' })['total_results']
    total_slices = int(math.ceil(float(total_leads) / 1000))
    slices = range(1, total_slices + 1)
    pool.map(_get_leads_slice, slices)
    return leads

def _get_calls_for_lead(lead):
    """Generate a list of all calls per lead"""
    print(f"Getting calls for {lead['display_name']}")
    has_more = True
    offset = 0
    calls = []
    while has_more:
        try:
            resp = api.get('activity/call', params={ 'lead_id': lead['id'], '_skip': offset, '_fields': 'duration,user_id' })
            calls += [i for i in resp['data']]
            offset += len(resp['data'])
            has_more = resp['has_more']
        except Exception as e:
            has_more = False 
    calls_per_lead.append({ 'lead': lead, 'calls': calls })
    

def _get_call_duration_per_lead():
    """Generate a list of call durations per lead per user"""
    _get_all_leads()
    pool.map(_get_calls_for_lead, leads)
    final_calls = []
    for item in calls_per_lead:
        lead_data = {'Lead ID': item['lead']['id'], 'Lead Name': item['lead']['display_name']}
        lead_data['Total Talk Time'] = pretty_time(sum([i['duration'] for i in item['calls']]))
        for k, v in user_ids_to_names.items():
            lead_data[f'{v} Total Talk Time'] = pretty_time(sum([i['duration'] for i in item['calls'] if i['user_id'] == k]))
        final_calls.append(lead_data)
    return final_calls
        
def export_total_talk_time_per_lead_for_each_org():
    """For each api key given, upload a CSV to dropbox of the total talk
    time per lead per user for an organization. 
    """
    global leads
    global calls_per_lead
    global user_ids_to_names
    global api
    for api_key in os.environ.get('CLOSE_API_KEYS').split(','):
        ## Initiate Close API
        leads = []
        calls_per_lead = []
        user_ids_to_names = {}
        api = CloseIO_API(api_key.strip())
        try:
            org = api.get('me')['organizations'][0]
            org_name = org['name'].replace('/', ' ')
            org_id = org['id']
            org_memberships = api.get('organization/' + org['id'], params={
                '_fields': 'memberships,inactive_memberships'
            })
            user_ids_to_names = { k['user_id'] : k['user_full_name'] for k in org_memberships['memberships'] + org_memberships['inactive_memberships'] }
        except APIError as e:
            print(f'Failed to pull org data because {str(e)} for {api_key}')
            continue
        
        try:
            name_keys = [f'{v} Total Talk Time' for v in user_ids_to_names.values()]
            name_keys = sorted(name_keys)
            print(f'Getting calls for {org_name}')
            final_calls_per_lead = _get_call_duration_per_lead()
            final_calls_per_lead = sorted(final_calls_per_lead, key=itemgetter('Lead Name'))
        except Exception as e:
            print(f'Failed to pull calls for {org_name} because {str(e)}')
            continue
        
        ordered_keys = ['Lead ID', 'Lead Name', 'Total Talk Time'] + name_keys
        output = io.StringIO()
        writer = csv.DictWriter(output, ordered_keys)
        writer.writeheader()
        writer.writerows(final_calls_per_lead)
        csv_output = output.getvalue().encode('utf-8')
        
        file_name = f"{org_name}/{org_name} Total Talk Time {datetime.today().strftime('%Y-%m-%d')}.csv"
        upload_to_dropbox(file_name, csv_output)
        
        
            
            
    