from interfaces import mongodb_requests
from interfaces import root_service_manager_requests
import copy
import os
import logging

from interfaces.mongodb_requests import mongo_update_job_instance

CLUSTER_ID = os.environ.get("CLUSTER_ID", "")


def service_resolution(service_name):
    """
    Resolves the service instance list by service name with the local DB,
    if no result found the query is propagated to the System Manager

    returns:
        instance_list: [{
                        instance_number: int
                        instance_ip: string
                        instance_ip_v6: string
                        namespace_ip: string
                        namespace_ip_v6: string
                        host_ip: string
                        host_port: string
                        }]
        service_ip_list: [{
                            IpType: string
                            Address: string
                            Address_v6: string
                        }]
    """
    # resolve it locally
    job = mongodb_requests.mongo_find_job_by_name(service_name)
    instances = None
    siplist = None

    # if no results, ask the root orc
    if job is None:
        job = root_service_manager_requests.cloud_table_query_service_name(service_name)
        instances = job['instance_list']
        siplist = job['service_ip_list']
        mongodb_requests.mongo_insert_job(copy.deepcopy(job))
        for instance in instances:
            mongo_update_job_instance(job['job_name'], instance)
    else:
        instances = job['instance_list']
        siplist = job['service_ip_list']

    return instances, siplist


def service_resolution_ip(ip_string):
    """
    Resolves the service instance list by service ServiceIP with the local DB,
    if no result found the query is propagated to the System Manager

    returns:
        name: string #service name
        instance_list: [{
                        instance_number: int
                        namespace_ip: string
                        namespace_ip_v6: string
                        host_ip: string
                        host_port: string
                    }]
        service_ip_list: [{
                                IpType: string
                                Address: string
                                Address_v6: string
                    }]

    """
    # resolve it locally
    job = mongodb_requests.mongo_find_job_by_ip(ip_string)

    # if no results, ask the root orc
    if job is None:
        job = root_service_manager_requests.cloud_table_query_ip(ip_string)
        mongodb_requests.mongo_insert_job(copy.deepcopy(job))
        for instance in job.get('instance_list'):
            mongo_update_job_instance(job['job_name'], instance)
    return job.get("job_name"), job.get('instance_list'), job.get('service_ip_list')


def format_instance_response(instance_list, sip_list):
    for elem in instance_list:
        # attach service IPs
        elem['service_ip'] = copy.deepcopy(sip_list)
        elem['service_ip'].append({
            "IpType": "instance_ip",
            "Address": elem['instance_ip'],
            "Address_v6": elem['instance_ip_v6']
        })
        # annotate cluster id for cluster-aware routing
        # ensure load_metrics present if tracked in DB
        # - If the DB contains a dict, normalize keys so JSON serialization is predictable
        # - If the value is missing or explicitly null, remove the key so callers receive no field
        lm = elem.get('load_metrics')
        print("TABLEQUERY - raw load_metrics for %s: %s", elem.get('instance_ip'), lm)
        if isinstance(lm, dict):
            # normalize/whitelist keys to avoid passing DB-specific types
            normalized = {
                'cpu_usage': lm.get('cpu_usage', 0),
                'memory_usage': lm.get('memory_usage', 0),
                'active_connections': lm.get('active_connections', -1),
                'timestamp': lm.get('timestamp'),
            }
            # propagate cluster_id if present inside load_metrics
            if lm.get('cluster_id') is not None:
                normalized['cluster_id'] = lm.get('cluster_id')
                elem['cluster_id'] = lm.get('cluster_id')
            elem['load_metrics'] = normalized
            logging.debug("TABLEQUERY - attached load_metrics for %s: %s", elem.get('instance_ip'), normalized)
        elif lm is None:
            # remove explicit nulls so JSON will omit the field (Go side uses `omitempty`)
            if 'load_metrics' in elem:
                try:
                    del elem['load_metrics']
                except Exception:
                    pass
            logging.debug("TABLEQUERY - no load_metrics for %s", elem.get('instance_ip'))
        else:
            # Some BSON types returned by PyMongo may not be plain dicts but still behave like mappings.
            # Try to normalize by accessing expected keys; if that fails, leave the original value untouched.
            try:
                normalized = {
                    'cpu_usage': lm.get('cpu_usage', 0),
                    'memory_usage': lm.get('memory_usage', 0),
                    'active_connections': lm.get('active_connections', -1),
                    'timestamp': lm.get('timestamp'),
                }
                if lm.get('cluster_id') is not None:
                    normalized['cluster_id'] = lm.get('cluster_id')
                    elem['cluster_id'] = lm.get('cluster_id')
                elem['load_metrics'] = normalized
                logging.debug("TABLEQUERY - normalized non-dict load_metrics for %s: %s", elem.get('instance_ip'), normalized)
            except Exception:
                # leave as-is (best-effort) so callers can still inspect raw value
                logging.debug("TABLEQUERY - unexpected load_metrics type for %s, leaving raw value", elem.get('instance_ip'))
    return instance_list
