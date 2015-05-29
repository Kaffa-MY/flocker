=============
Release Notes
=============

.. note:: If you already have a tutorial environment from a previous release see :ref:`upgrading-vagrant-environment`.

.. note:: If you already have an old version of ``flocker-cli`` installed, delete the install script and directory before installing the newer version.

Next Release
============

* ``flocker-deploy`` now takes an extra argument, the hostname of the control service.
* It is now necessary to specify a dataset backend for each agent node.
  See :ref:`post-installation-configuration`.
* Flocker-initiated communication is secured with TLS.
  See :ref:`authentication`.
* Docker restart policies are adhered to.
* New API endpoint: List the hosts currently in the cluster.
  See :ref:`api`.

v0.4
====

* New :ref:`REST API<api>` for managing datasets.
* Applications can now be configured with a :ref:`restart policy<restart configuration>`.
* Volumes can now be configured with a :ref:`maximum size<volume configuration>`.
* Documentation now includes :ref:`instructions for installing flocker-node on CentOS 7<centos-7-install>`.
* SELinux must be disabled before installing Flocker.
  A future version of Flocker may provide a different integration strategy.

v0.3.2
======

* Documented how to :ref:`configure the Fedora firewall<fedora-20-install>` on certain cloud platforms.


v0.3.1
======

* Applications can now be :ref:`configured with a CPU and memory limit<configuration>`.
* Documentation now includes :ref:`instructions for installing flocker-node on Fedora20<fedora-20-install>`.
* Documentation now includes instructions for deploying ``flocker-node`` on three popular cloud services: :ref:`Amazon EC2<aws-install>`, :ref:`Rackspace<rackspace-install>`, and DigitalOcean.


v0.3
====

* ``geard`` is no longer used to manage Docker containers.
* Added support for `Fig`_ compatible :ref:`application configuration <fig-compatible-config>` files.


v0.2
====

* Moving volumes between nodes is now done with a :ref:`two-phase push<clustering>` that should dramatically decrease application downtime when moving large amounts of data.
* Added support for environment variables in the :ref:`application configuration<configuration>`.
* Added basic support for links between containers in the :ref:`application configuration<configuration>`.

v0.1
====

Everything is new since this is our first release.


Known Limitations
=================

* This release is not ready for production and should not be used on publicly accessible servers or to store data you care about.
  Backwards compatibility is not a goal yet.
* Changes to the application configuration file will often not be noticed by ``flocker-deploy``, and there is no way to delete applications or volumes.
  Choose new names for your applications if you are making changes to the application configuration.

You can learn more about where we might be going with future releases by:

* Stopping by the ``#clusterhq`` channel on ``irc.freenode.net``.
* Visiting our GitHub repository at https://github.com/ClusterHQ/flocker.

.. _`Fig`: http://www.fig.sh/yml.html