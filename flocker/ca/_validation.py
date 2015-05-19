# Copyright ClusterHQ Inc.  See LICENSE file for details.

"""
TLS context factories used to validate the various kinds of
certificates generated by the CA.

We rely on Twisted's ``CertificateOptions`` to provide certain defaults,
e.g. TLS only (no SSL).
"""

from OpenSSL.SSL import VERIFY_PEER, VERIFY_FAIL_IF_NO_PEER_CERT

from zope.interface import implementer

from twisted.web.iweb import IPolicyForHTTPS
from twisted.internet.ssl import optionsForClientTLS

from pyrsistent import PRecord, field


@implementer(IPolicyForHTTPS)
class ControlServicePolicy(PRecord):
    """
    HTTPS TLS policy for validating the control service's identity, and
    providing the HTTP client's identity.

    :ivar Certificate ca_certificate: The certificate authority's
        certificate.

    :ivar PrivateCertificate client_certificate: Client's
        certificate/private key pair.
    """
    ca_certificate = field(mandatory=True)
    client_certificate = field(mandatory=True)

    def creatorForNetloc(self, hostname, port):
        return optionsForClientTLS(u"control-service",
                                   trustRoot=self.ca_certificate,
                                   clientCertificate=self.client_certificate)


class _ClientContextFactory(object):
    """
    Context factory that validates various kinds of clients that can
    connect to the control service.
    """
    def __init__(self, ca_certificate, control_credential, prefix):
        """
        :param Certificate ca_certificate: The certificate authority's
            certificate.

        :param ControlCredential control_credential: The control service's
            credentials.

        :param bytes prefix: The required prefix on certificate common names.
        """
        self.prefix = prefix
        self._default_options = control_credential._default_options(
            ca_certificate)

    def getContext(self):
        def verify(conn, cert, errno, depth, preverify_ok):
            if depth > 0:
                # Certificate authority chain:
                return preverify_ok
            # Now we're actually verifying certificate we care about:
            if not preverify_ok:
                return preverify_ok
            return cert.get_subject().commonName.startswith(self.prefix)
        context = self._default_options.getContext()
        context.set_verify(VERIFY_PEER | VERIFY_FAIL_IF_NO_PEER_CERT,
                           verify)
        return context


def amp_server_context_factory(ca_certificate, control_credential):
    """
    Create a context factory that validates node agents connecting to the
    control service.

    :param Certificate ca_certificate: The certificate authority's
        certificate.

    :param ControlCredential control_credential: The control service's
        credentials.

    :return: TLS context factory suitable for use by the control service
        AMP server.
    """
    return _ClientContextFactory(ca_certificate, control_credential, b"node-")


def rest_api_context_factory(ca_certificate, control_credential):
    """
    Create a context factory that validates REST API clients connecting to
    the control service.

    :param Certificate ca_certificate: The certificate authority's
        certificate.

    :param ControlCredential control_credential: The control service's
        credentials.

    :return: TLS context factory suitable for use by the control service
        REST API server.
    """
    return _ClientContextFactory(ca_certificate, control_credential, b"user-")