# -*- coding: utf-8 -*-

import sys
import unittest
import tempfile

import networkx as nx

sys.path.append('../../identify')

from hat.fs import HadoopFS

from area import make_network

from pagerank import pagerank

class AreaTestCase(unittest.TestCase):
    def setUp(self):
        self.top_n = 500
        self.date = '2013-3-1'
        self.topic = u'两会'
        self.window_size = 1
        self.iter_count = 5
        self.graph_data = None
        
    def test_make_network(self):
        g = make_network(self.topic, self.date, self.window_size, max_size=100000, ts=False)
        self.graph_data = g
        self.assertIsInstance(g, nx.DiGraph, 'incorrect graph type')
        self.assertIsNotNone(g, 'graph should exist')
        self.assertNotEqual(len(g.nodes()), 0, 'graph should have nodes')
        self.assertNotEqual(len(g.edges()), 0, 'graph should have edges')

    def test_pagerank(self):
        size = 1000
        g = nx.DiGraph(nx.powerlaw_cluster_graph(size, 3, 0.001))
        N = len(g.nodes())
        tmp_file = tempfile.NamedTemporaryFile(delete=False)
        for node in g.nodes():
            outlinks = g.out_edges(nbunch=[node])
            outlinks = map(str, [n2 for n1, n2 in outlinks])
            if not outlinks:
                value = 'pr_results,%s,%s' % (1.0/N, N)
                tmp_file.write('%s\t%s\n' % (node, value))
            else:
                outlinks_str = ','.join(outlinks)
                value = 'pr_results,%s,%s,' % (1.0/N, N)
                value += outlinks_str
                tmp_file.write('%s\t%s\n' % (node, value))
        tmp_file.flush()
        input_path = tmp_file.name
        job_id = 'unittest'
        sorted_ids = pagerank(job_id, self.iter_count, input_path, self.top_n)
        fs = HadoopFS()
        fs.rmr('%s/hat_results' % job_id)
        if self.top_n <= size: 
            self.assertEqual(len(sorted_ids), self.top_n, 'some ids is missing')
        id_ranges = range(0, 1000)
        for _id in sorted_ids:
            self.assertIn(int(_id), id_ranges, 'node should in graph')


if __name__ == '__main__':
    unittest.main()
