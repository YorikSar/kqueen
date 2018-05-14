from kqueen.engines import openstack_kubespray

import yaml
import unittest


class TestHeat(unittest.TestCase):

    def test__get_template_and_files(self):
        template, files = openstack_kubespray.get_template_and_files()
        self.assertTrue(files)
        with open(template) as tf:
            tpl = yaml.safe_load(tf)
        self.assertIn("heat_template_version", tpl)
        for f in files:
            filename = f.rsplit("file://")[1]
            with open(filename) as tf:
                tpl = yaml.safe_load(tf)
            self.assertIn("heat_template_version", tpl)
