from network.subnetwork_management import *
from interfaces.mongodb_requests import *
from utils.sla_validation import check_valid_sla

@check_valid_sla
def deploy_request(deployment_descriptor=None, system_job_id=None):
    if system_job_id is None:
        return "Invalid system_job_id", 400

    s_ip = [{
        "IpType": 'RR',
        "Address": new_job_rr_address(deployment_descriptor),
        "Address_v6": new_job_rr_address_v6(deployment_descriptor)
    }]

    # If SLA provides cluster-aware semantic IPs, include them as ServiceIP entries
    ca_ip = deployment_descriptor.get("ClusterAware_ip")
    ca_ip_v6 = deployment_descriptor.get("ClusterAware_ip_v6")
    if ca_ip is not None or ca_ip_v6 is not None:
        s_ip.append({
            "IpType": 'ClusterAware',
            "Address": ca_ip if ca_ip is not None else "",
            "Address_v6": ca_ip_v6 if ca_ip_v6 is not None else ""
        })
    job_id = mongo_insert_job(
        {
            'system_job_id': system_job_id,
            'deployment_descriptor': deployment_descriptor,
            'service_ip_list': s_ip
        })
    return "Instance info added", 200


def remove_service(system_job_id=None):
    if system_job_id is None:
        return "Invalid input parameters", 400

    job = mongo_find_job_by_systemid(system_job_id)

    if job is None:
        return "Invalid input parameters", 400

    instances = job.get("instance_list")

    if instances is not None:
        if len(instances) > 0:
            return "There are services still deployed", 400

    mongo_remove_job(system_job_id)
    return "Job removed successfully", 200
