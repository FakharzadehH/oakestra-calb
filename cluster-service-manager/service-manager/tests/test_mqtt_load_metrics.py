import unittest
import sys
import types
import importlib.machinery

# Provide stub for paho.mqtt.client
paho_stub_parent = types.ModuleType('paho')
paho_stub = types.ModuleType('paho.mqtt')
paho_client_stub = types.ModuleType('paho.mqtt.client')
class _Dummy:
    def __init__(self,*a,**k): pass
    def __getattr__(self, name): return self
    def __call__(self,*a,**k): return self
Client = _Dummy
paho_client_stub.Client = Client
sys.modules['paho'] = paho_stub_parent
sys.modules['paho.mqtt'] = paho_stub
sys.modules['paho.mqtt.client'] = paho_client_stub

# Create a stub for interfaces.mongodb_requests before importing mqtt_client to avoid external deps
stub_mod = types.ModuleType('interfaces.mongodb_requests')
def _stub_update(job_name, instance_number, load_metrics):
    pass
stub_mod.mongo_update_instance_load_metrics = _stub_update
stub_mod.mongo_find_node_by_id_and_update_subnetwork = lambda *a, **k: None
stub_mod.mongo_update_job_instance = lambda *a, **k: None
stub_mod.mongo_update_job_deployed = lambda *a, **k: None
stub_mod.mongo_find_job_by_name = lambda *a, **k: None
stub_mod.mongo_insert_job = lambda *a, **k: None
stub_mod.mongo_find_job_by_ip = lambda *a, **k: None
stub_mod.mongo_remove_job = lambda *a, **k: None
stub_mod.mongo_remove_interest = lambda *a, **k: None
stub_mod.mongo_add_interest = lambda *a, **k: None
stub_mod.mongo_get_interest_workers = lambda *a, **k: []
sys.modules['interfaces.mongodb_requests'] = stub_mod

from interfaces import mqtt_client


class DummyApp:
    def __init__(self):
        class L:
            def info(self, *a, **k):
                pass

            def error(self, *a, **k):
                pass

        self.logger = L()


actions = []


def fake_mongo_update(job_name, instance_number, load_metrics):
    actions.append((job_name, instance_number, load_metrics))


class TestLoadMetricsHandler(unittest.TestCase):
    def test_basic(self):
        mqtt_client.app = DummyApp()
        actions.clear()
        mqtt_client.mongo_update_instance_load_metrics = fake_mongo_update
        payload = {"load_metrics": [
            {"job_name": "svc.a", "instance_number": 0, "cpu_usage": 0.1, "memory_usage": 0.2, "active_connections": 5},
            {"job_name": "svc.b", "instance_number": 1, "cpu_usage": 0.3, "memory_usage": 0.4, "active_connections": -1},
        ]}
        mqtt_client._load_metrics_handler("node1", payload)
        self.assertEqual(len(actions), 2)
        self.assertEqual(actions[0][0], 'svc.a')
        self.assertEqual(actions[0][2]['cpu_usage'], 0.1)
        self.assertEqual(actions[1][0], 'svc.b')

    def test_ignores_bad_payload(self):
        mqtt_client.app = DummyApp()
        actions.clear()
        mqtt_client.mongo_update_instance_load_metrics = fake_mongo_update
        mqtt_client._load_metrics_handler("node1", {"load_metrics": {"bad": "dict"}})
        self.assertEqual(len(actions), 0)
        mqtt_client._load_metrics_handler("node1", {"other": []})
        self.assertEqual(len(actions), 0)


if __name__ == '__main__':
    unittest.main()
