"""Parsing of the x.509 options (`crl_*`, `client_cert_auth_state`) in the `[cluster]` section."""

from unittest import TestCase

from perfrunner.settings import ClientCertAuthState, ClusterSettings, CRLMode


class CrlSettingsTest(TestCase):

    def test_unset_options_are_none(self):
        settings = ClusterSettings({})

        self.assertIsNone(settings.crl_client_auth_mode)
        self.assertIsNone(settings.crl_node_to_node_mode)
        self.assertIsNone(settings.crl_check_intermediate_certs)

    def test_check_intermediate_certs_accepts_the_usual_boolean_spellings(self):
        for value, expected in (("true", True), ("False", False), ("1", True), ("0", False)):
            with self.subTest(value=value):
                settings = ClusterSettings({"crl_check_intermediate_certs": value})
                self.assertIs(settings.crl_check_intermediate_certs, expected)

    def test_enum_options_are_case_insensitive_and_reject_unknown_values(self):
        settings = ClusterSettings(
            {
                "crl_client_auth_mode": "require",
                "crl_node_to_node_mode": "DISABLED",
                "client_cert_auth_state": "Mandatory",
            }
        )

        self.assertIs(settings.crl_client_auth_mode, CRLMode.REQUIRE)
        self.assertIs(settings.crl_node_to_node_mode, CRLMode.DISABLED)
        self.assertIs(settings.client_cert_auth_state, ClientCertAuthState.MANDATORY)
        with self.assertRaises(ValueError):
            ClusterSettings({"crl_node_to_node_mode": "strict"})
