# Copyright ClusterHQ Inc.  See LICENSE file for details.

"""
Tests for restarting and reboots and their interactions.
"""

import os

from subprocess import call, check_call, CalledProcessError
from unittest import SkipTest

from twisted.python.filepath import FilePath
from twisted.trial.unittest import TestCase, FailTest

from treq import get, content

from ...testtools import loop_until, random_name
from ..testtools import create_dataset, require_cluster
from ...common.runner import run_ssh


REBOOT_SERVER = FilePath(__file__).sibling(b"reboot_httpserver.py")

def _service(address, name, action):
    """
    :param bytes address: The public IP of the node on which the service will
        be changed.
    :param bytes action: The action to perform on the service.
    """
    from twisted.internet import reactor
    command = ["systemctl", action, name]
    d = run_ssh(reactor, b"root", address, command)

    def handle_error(_, action):
        raise FailTest(
            "{} failed. See logs for process output.".format(
                command
            )
        )

    d.addErrback(handle_error, action)
    return d


class RebootTests(TestCase):
    """
    Tests for rebooting machines.
    """
    @require_cluster(2)
    def test_restart_always_reboot_with_dataset(self, cluster):
        """
        If a container has a volume mapped to a dataset, on reboots it will
        only be restarted after the volume becomes available.

        This test is designed to catch the following bug:
        * Server reboots.
        * Container agent starts before dataset agent has reported its state.
        * On connecting to control service it receives stale dataset state from
          before the reboot. (The stale state wiper has not yet purged the
          stale dataset state)
        * Container agent acts on this state by starting a container.
        * Docker finds the (unmounted) /flocker/<dataset_id> subdirectory from
          before the reboot.
        * Stateful application begins writing data to a Docker AUFs layer which
          will be lost upon next container restart.
        """
        # Find a node which is not running the control service.
        # If the control node is rebooted, we won't get stale dataset state.
        node = [node for node in cluster.nodes if
                node.public_address != cluster.control_node.public_address][0]
        print "OPERATING ON:", node
        creating_dataset = create_dataset(self, cluster, node=node)

        def query_server():
            req = get(
                "http://{host}:12345".format(host=node.public_address),
                persistent=False
            ).addCallbacks(content)
            return req

        def created_dataset(dataset):
            print "DATASET", dataset
            http_server = {
                u"name": random_name(self),
                u"node_uuid": str(node.uuid),
                u"image": u"python:2.7-slim",
                u"ports": [{u"internal": 12345, u"external": 12345}],
                u"volumes": [{u"dataset_id": str(dataset.dataset_id),
                              u"mountpoint": u"/data"}],
                u"command_line": [u"python", u"-c",
                                  REBOOT_SERVER.getContent().decode("ascii"),
                                  u"/data"],
            }
            created = cluster.create_container(http_server)
            created.addCallback(lambda _: self.addCleanup(
                cluster.remove_container, http_server[u"name"]))
            created.addCallback(lambda _:
                                loop_until(lambda: query_server().addErrback(
                                    lambda _: False)))
            created.addCallback(lambda _: query_server())
            return created
        creating_dataset.addCallback(created_dataset)

        def server_started(initial_response):
            # We now have the initial response of the server. This should
            # be the initial boot time of the node, repeated twice.  Next,
            # we restart the machine.
            print "INITIAL RESPONSE", initial_response
            initial_reboot_time = initial_response.splitlines()[0]
            initial_container_id = initial_response.splitlines()[2].encode("ascii")
            # XXX Checking exit code is problematic insofar as reboot

            # kills the ssh process...
            print "Rebooting!"
            disabling = _service(node.public_address, b'flocker-dataset-agent', b'disable')
            def reboot(ignored):
                call([b"ssh", b"root@{}".format(node.public_address),
                      b"shutdown", b"-r", b"now"])
            rebooting = disabling.addCallback(reboot)

            # Now, keep trying to get a response until it's different than
            # the one we originally got, thus indicating a reboot:
            def query_until_different():
                print "query!"
                return query_server().addCallbacks(
                    lambda response: response != initial_response,
                    lambda _: False)

            # Now, poll the host until the now-stopped Docker container that
            # existed on the first run is destroyed. This is an external sign
            # that the container agent tried to "restart" (by destroying and
            # recreating) the container.
            #
            def old_container_gone():
                # This is only necessary to allow the fixed code to proceed.  A
                # simpler test against master shows that the container-agent
                # does start the container before the dataset is mounted and
                # the webserver returns the new uptime.
                if call([b"ssh", b"root@{}".format(node.public_address),
                         b"true"]) == 0:
                    if call([b"ssh", b"root@{}".format(node.public_address),
                             b"docker", b"inspect", initial_container_id]) == 0:
                        print "Container", initial_container_id, "still exists..."
                        return False
                    else:
                        print "Container", initial_container_id, "stopped existing!"
                        return True
                else:
                    print "Failed to connect this time, trying again..."
                    return False

            gone = rebooting.addCallback(lambda _: loop_until(old_container_gone))
            enabled = gone.addCallback(
                lambda _: _service(node.public_address, b'flocker-dataset-agent', b'enable')
            )
            started = enabled.addCallback(
                lambda _: _service(node.public_address, b'flocker-dataset-agent', b'start')
            )
            different = started.addCallback(lambda _: loop_until(query_until_different))
            queried = different.addCallback(lambda _: query_server())

            # Now that we've rebooted, we expect first line to be
            # unchanged (i.e. preserved across reboots in the volume):
            def got_rebooted_response(second_response):
                print "SECOND RESPONSE", second_response
                preserved_initial_reboot_time = second_response.splitlines()[0]
                new_container_id = second_response.splitlines()[2]
                self.assertEqual(initial_reboot_time,
                                 preserved_initial_reboot_time)
                self.assertNotEqual(initial_container_id, new_container_id)
            queried.addCallback(got_rebooted_response)
            return queried
        creating_dataset.addCallback(server_started)
        return creating_dataset

    # Reboots can take a while:
    test_restart_always_reboot_with_dataset.timeout = 480