import importlib
import os
from pathlib import Path
import tempfile
import tomllib
import unittest
from unittest.mock import patch
from xml.etree import ElementTree

from scripts.datasource_config import append_datasource


class DatasourceConfigTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name)
        self.config = self.directory / "deployment.toml"
        self.url = "jdbc:postgresql://db.test/benchmark?sslmode=disable&ApplicationName=client"
        self.config.write_text(
            """
version = 2
[single]
datasource = 'benchmark'
[datasources.benchmark]
url = '%s'
username = 'client&reader'
password = 'secret<&value'
driver = 'example.CustomDriver'
type = 'POSTGRES'
""" % self.url,
            encoding="utf-8",
        )

    def test_reads_selected_toml_datasource_without_double_escaping_xml(self):
        root = ElementTree.Element("parameters")
        append_datasource(root, self.config)
        serialized = ElementTree.tostring(root, encoding="unicode")
        self.assertIn("&amp;ApplicationName", serialized)
        self.assertNotIn("&amp;amp;", serialized)
        parsed = ElementTree.fromstring(serialized)
        self.assertEqual(self.url, parsed.findtext("url"))
        self.assertEqual("client&reader", parsed.findtext("username"))
        self.assertEqual("secret<&value", parsed.findtext("password"))
        self.assertEqual("example.CustomDriver", parsed.findtext("driver"))
        self.assertEqual("POSTGRES", parsed.findtext("type"))

    def test_environment_override_and_explicit_path_precedence(self):
        with patch.dict(os.environ, {"TXNSAILS_CONFIG": str(self.config)}):
            root = ElementTree.Element("parameters")
            append_datasource(root)
            self.assertEqual(self.url, root.findtext("url"))
        with patch.dict(os.environ, {"TXNSAILS_CONFIG": str(self.directory / "missing.toml")}):
            root = ElementTree.Element("parameters")
            append_datasource(root, self.config)
            self.assertEqual(self.url, root.findtext("url"))

    def test_defaults_follow_named_datasource_and_missing_references_fail_clearly(self):
        minimal = self.directory / "minimal.toml"
        minimal.write_text(
            "version = 2\n[single]\ndatasource = 'db'\n[datasources.db]\nurl = 'jdbc:postgresql://db.test/data'\n",
            encoding="utf-8",
        )
        root = ElementTree.Element("parameters")
        append_datasource(root, minimal)
        self.assertEqual("org.postgresql.Driver", root.findtext("driver"))
        self.assertEqual("POSTGRES", root.findtext("type"))
        self.assertEqual("", root.find("username").text)
        self.assertEqual("", root.find("password").text)
        minimal.write_text("version = 2\n[single]\ndatasource = 'missing'\n", encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "datasource.*missing"):
            append_datasource(ElementTree.Element("parameters"), minimal)

    def test_invalid_version_url_and_credentials_are_rejected_before_appending(self):
        original = self.config.read_text(encoding="utf-8")
        for invalid in (
            original.replace("version = 2", "version = 1"),
            original.replace(self.url, "https://db.test/data"),
            original.replace("username = 'client&reader'", "username = 123"),
            original.replace("driver = 'example.CustomDriver'", "driver = ''"),
        ):
            with self.subTest(configuration=invalid.splitlines()[1:3]):
                self.config.write_text(invalid, encoding="utf-8")
                root = ElementTree.Element("parameters")
                with self.assertRaises(ValueError):
                    append_datasource(root, self.config)
                self.assertEqual(0, len(root))

    def test_default_config_path_is_relative_to_repository_instead_of_working_directory(self):
        repository = Path(__file__).resolve().parents[2]
        with (repository / "config" / "txnsails.toml").open("rb") as source:
            document = tomllib.load(source)
        expected = document["datasources"][document["single"]["datasource"]]["url"]
        previous = Path.cwd()
        try:
            os.chdir(self.directory)
            with patch.dict(os.environ):
                os.environ.pop("TXNSAILS_CONFIG", None)
                root = ElementTree.Element("parameters")
                append_datasource(root)
                self.assertEqual(expected, root.findtext("url"))
        finally:
            os.chdir(previous)

    def test_all_benchmark_generators_use_toml_connection_settings_and_preserve_workload(self):
        cases = (
            ("gen_ycsb_config", "generate_mysql_ycsb_config", ("SERIALIZABLE", 0.3, 0.5, 0.7, 4, [0, 0, 0, 0, 0, 0, 100])),
            ("gen_sb_config", "generate_pg_sb_config", ("SI_TAILOR", 4, [20, 20, 20, 0, 20, 20])),
            ("gen_tpcc_config", "generate_mysql_tpcc_config", ("RC_TAILOR", 4, [45, 43, 4, 4, 4])),
        )
        with patch.dict(os.environ, {"TXNSAILS_CONFIG": str(self.config)}):
            for module_name, function_name, args in cases:
                with self.subTest(benchmark=module_name):
                    module = importlib.import_module("scripts." + module_name)
                    output = self.directory / module_name
                    output.mkdir()
                    getattr(module, function_name)(*args, dir=str(output))
                    files = list(output.glob("*.xml"))
                    self.assertEqual(1, len(files))
                    xml = files[0].read_text(encoding="utf-8")
                    root = ElementTree.fromstring(xml)
                    self.assertEqual(self.url, root.findtext("url"))
                    self.assertEqual("client&reader", root.findtext("username"))
                    self.assertEqual("secret<&value", root.findtext("password"))
                    self.assertEqual("example.CustomDriver", root.findtext("driver"))
                    self.assertEqual(1, len(root.findall("url")))
                    self.assertEqual(args[0], root.findtext("concurrencyControlType"))
                    self.assertEqual(str(args[-1])[1:-1], root.findtext("works/work/weights"))
                    self.assertNotIn("&amp;amp;", xml)


if __name__ == "__main__":
    unittest.main()
