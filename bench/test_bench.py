import json
import unittest
import run_execution as bench


class BenchmarkChecks(unittest.TestCase):
    def test_publication_hides_local_paths_and_device_identity(self):
        from report import public_hardware
        raw = {'command': [str(bench.ROOT / 'bench/workload.py')], 'workload_sha256': 'measured-hash', 'attempts': 2}
        published = bench.public_result(raw)
        self.assertEqual(published['command'], ['$PROJECT_ROOT/bench/workload.py'])
        self.assertEqual(published['workload_sha256'], raw['workload_sha256'])
        self.assertEqual(published['attempts'], 2)
        self.assertNotEqual(raw['command'], published['command'])
        self.assertEqual(public_hardware({'gpus': [{'id': 'private-device', 'memory_mib': 6144}]})['gpus'], [{'id': 'GPU-REDACTED-0', 'memory_mib': 6144}])

    def test_comparison_dispersion(self):
        from compare_improvement import describe
        result=describe([3,1,2])
        self.assertEqual(result['median'],2)
        self.assertEqual((result['min'],result['max'],result['p95']),(1,3,3))
        self.assertGreater(result['stdev'],0)

    def test_request_observer_counts_real_forwarded_requests(self):
        from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
        import threading
        from control_load import Observer
        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                self.rfile.read(int(self.headers['Content-Length']))
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b'{"ok":true}')
            def log_message(self,*args):
                pass
        server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
        thread=threading.Thread(target=server.serve_forever,daemon=True)
        thread.start()
        observer=Observer(f'127.0.0.1:{server.server_port}')
        try:
            self.assertEqual(bench.api(observer.url,'test','/worker/poll',{}),{'ok':True})
            self.assertEqual(observer.summary()['/worker/poll']['count'],1)
            observer.reset()
            self.assertEqual(observer.summary(),{})
        finally:
            observer.close()
            server.shutdown()
            server.server_close()
            thread.join()

    def test_identical_inputs_and_safe_limits(self):
        for scenario in bench.SCENARIOS:
            jobs = bench.workload(scenario, 42)
            self.assertEqual(json.dumps(jobs), json.dumps(bench.workload(scenario, 42)))
            self.assertEqual(len({j['spec']['submission_key'] for j in jobs}), len(jobs))
            for j in jobs:
                self.assertLessEqual(j['spec']['resources']['cpu'], 1)
                self.assertLessEqual(j['spec']['resources']['mem_mib'], 256)
                self.assertLessEqual(j['spec']['timeout_secs'], 12)
                self.assertTrue(j['spec']['trusted'])
                self.assertIsNone(j['spec']['image'])
            self.assertEqual([j['arrival_s'] for j in jobs], sorted(j['arrival_s'] for j in jobs))

    def test_process_sampling_and_quantiles(self):
        import os
        observed = bench.read_proc(os.getpid())
        self.assertGreater(observed['rss'], 0)
        self.assertGreater(observed['start'], 0)
        self.assertIsNone(bench.read_proc(2**31 - 1))
        self.assertEqual(bench.quantile([9, 1, 5], .5), 5)
        self.assertEqual(bench.quantile([9, 1, 5], .95), 9)


if __name__ == '__main__':
    unittest.main()
