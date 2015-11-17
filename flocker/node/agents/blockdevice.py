# -*- test-case-name: flocker.node.agents.test.test_blockdevice -*-
# Copyright Hybrid Logic Ltd.  See LICENSE file for details.

"""
This module implements the parts of a block-device based dataset
convergence agent that can be re-used against many different kinds of block
devices.
"""

from uuid import UUID
from subprocess import CalledProcessError, check_output, STDOUT
from stat import S_IRWXU, S_IRWXG, S_IRWXO
from errno import EEXIST
from datetime import timedelta

from bitmath import GiB

from eliot import MessageType, ActionType, Field, Logger
from eliot.serializers import identity

from zope.interface import implementer, Interface

from pyrsistent import PRecord, PClass, field, pmap_field, pset_field

import psutil

from twisted.python.reflect import safe_repr
from twisted.internet.defer import succeed, fail
from twisted.python.filepath import FilePath
from twisted.python.components import proxyForInterface
from twisted.python.constants import (
    Values, ValueConstant,
    Names, NamedConstant,
)

from .. import (
    IDeployer, ILocalState, IStateChange, sequentially, in_parallel,
    run_state_change, NoOp,
)
from .._deploy import NotInUseDatasets

from ...control import NodeState, Manifestation, Dataset, NonManifestDatasets
from ...control._model import pvector_field
from ...common import auto_threaded


# Eliot is transitioning away from the "Logger instances all over the place"
# approach.  And it's hard to put Logger instances on PRecord subclasses which
# we have a lot of.  So just use this global logger for now.
_logger = Logger()

# The size which will be assigned to datasets with an unspecified
# maximum_size.
# XXX: Make this configurable. FLOC-2679
DEFAULT_DATASET_SIZE = int(GiB(100).to_Byte().value)

# The metadata key for flocker profiles.
PROFILE_METADATA_KEY = u"clusterhq:flocker:profile"


class DatasetStates(Names):
    """
    States that a ``Dataset`` can be in.

    """
    # Doesn't exist yet.
    NON_EXISTENT = NamedConstant()
    # Exists, but attached elsewhere
    ATTACHED_ELSEWHERE = NamedConstant()
    # Exists, but not attached
    NON_MANIFEST = NamedConstant()
    # Attached to this node but no filesystem
    ATTACHED_NO_FILESYSTEM = NamedConstant()
    # Attached to this node, has filesystem
    ATTACHED = NamedConstant()
    # Mounted on this node
    MOUNTED = NamedConstant()
    # Deleted from the driver
    DELETED = NamedConstant()


class DiscoveredDataset(PClass):
    """
    Dataset as discovered by deployer.

    :ivar DatasetStates state: The state this dataset was determined to be in.
    :ivar int maximum_size: The maximum size of the dataset.
    :param unicode blockdevice_id: The unique identifier of the
        ``IBlockDeviceAPI``-managed volume.
    :ivar FilePath device_path: The absolute path to the block device file on
        the node where the dataset is attached.
    :ivar FilePath mount_point: The absolute path to the location on the node
        where the dataset will be mounted.
    """
    state = field(
        invariant=lambda state: (state in DatasetStates.iterconstants(),
                                 "Not a valid state"),
        mandatory=True,
    )
    dataset_id = field(type=UUID, mandatory=True)
    maximum_size = field(type=int, mandatory=True)
    blockdevice_id = field(type=unicode, mandatory=True)
    device_path = field(FilePath)
    mount_point = field(FilePath)

    def __invariant__(self):
        """
        Check that the state is valid for a ``DiscoveredDataset`` and
        that all the attributes required for the state are specified.
        """
        expected_attributes = [
            ((DatasetStates.ATTACHED, DatasetStates.MOUNTED,
              DatasetStates.ATTACHED_NO_FILESYSTEM), "device_path"),
            ((DatasetStates.MOUNTED,), "mount_point"),
        ]
        for states, attribute in expected_attributes:
            if (self.state in states) != hasattr(self, attribute):
                if self.state in states:
                    message = (
                        "`{attr}` must be specified in state `{state}`"
                        .format(attr=attribute, state=self.state.name)
                    )
                else:
                    message = (
                        "`{attr}` can only be specified in states {states}"
                        .format(
                            attr=attribute,
                            states=','.join(map("`{0.name}`".format, states)),
                        )
                    )
                return (False, message)
        if self.state in (DatasetStates.DELETED, DatasetStates.NON_EXISTENT):
            return (False, "DesiredDataset can't be in state DELETED.")
        return (True, "")


class DesiredDataset(PClass):
    """
    Dataset as requested by configuration (and applications).
    """
    state = field(
        invariant=lambda state: (state in DatasetStates.iterconstants(),
                                 "Not a valid state"),
        mandatory=True,
    )
    dataset_id = field(type=UUID, mandatory=True)
    maximum_size = field(type=int)
    metadata = pmap_field(
        key_type=unicode,
        value_type=unicode,
    )
    mount_point = field(FilePath)

    def __invariant__(self):
        expected_attributes = [
            ((DatasetStates.NON_MANIFEST, DatasetStates.MOUNTED),
             "maximum_size"),
            ((DatasetStates.MOUNTED,),
             "mount_point"),
        ]
        for states, attribute in expected_attributes:
            if (self.state in states) != hasattr(self, attribute):
                if self.state in states:
                    message = (
                        "`{attr}` must be specified in state `{state}`"
                        .format(attr=attribute, state=self.state.name)
                    )
                else:
                    message = (
                        "`{attr}` can only be specified in states {states}"
                        .format(
                            attr=attribute,
                            states=','.join(map("`{0.name}`".format, states)),
                        )
                    )
                return (False, message)
        disallowed_states = (
            DatasetStates.ATTACHED, DatasetStates.NON_EXISTENT,
            DatasetStates.ATTACHED_ELSEWHERE,
            DatasetStates.ATTACHED_ELSEWHERE,
        )
        if self.state in disallowed_states:
            return (False, "{class_} can't be in states {states}.".format(
                class_=self.__class__,
                states=','.join(map("`{0.name}`".format, disallowed_states)),
            ))
        return (True, "")


class ICalculater(Interface):
    """
    An object that can calculate the changes required to bring dataset state
    and desired dataset configuration into alignment.
    """
    def calculate_changes_for_datasets(
        self, discovered_datasets, desired_datasets,
    ):
        """
        Calculate the state changes necessary to make the local state match the
        desired cluster configuration.

        :param discovered_datasets: The datasets that have been discovered.
        :type discovered_datasets: mapping of `dataset_id`` to
            ``DiscoveredDataset``.
        :param desired_datasets: The datasets that are desired on this node.
        :type desired_datasets: mapping of `dataset_id`` to ``DesiredDataset``.

        :return: An ``IStateChange`` provider.
        """


class VolumeException(Exception):
    """
    A base class for exceptions raised by  ``IBlockDeviceAPI`` operations.

    :param unicode blockdevice_id: The unique identifier of the
        ``IBlockDeviceAPI``-managed volume.
    """
    def __init__(self, blockdevice_id):
        if not isinstance(blockdevice_id, unicode):
            raise TypeError(
                'Unexpected blockdevice_id type. '
                'Expected unicode. '
                'Got {!r}.'.format(blockdevice_id)
            )
        Exception.__init__(self, blockdevice_id)
        self.blockdevice_id = blockdevice_id


class UnknownVolume(VolumeException):
    """
    The block device could not be found.
    """


class AlreadyAttachedVolume(VolumeException):
    """
    A failed attempt to attach a block device that is already attached.
    """


class UnattachedVolume(VolumeException):
    """
    An attempt was made to operate on an unattached volume but the operation
    requires the volume to be attached.
    """


class DatasetExists(Exception):
    """
    A ``BlockDeviceVolume`` with the requested dataset_id already exists.
    """
    def __init__(self, blockdevice):
        Exception.__init__(self, blockdevice)
        self.blockdevice = blockdevice


class FilesystemExists(Exception):
    """
    A failed attempt to create a filesystem on a block device that already has
    one.
    """
    def __init__(self, device):
        Exception.__init__(self, device)
        self.device = device


class UnknownInstanceID(Exception):
    """
    Could not compute instance ID for block device.
    """
    def __init__(self, blockdevice):
        Exception.__init__(
            self,
            'Could not find valid instance ID for {}'.format(blockdevice))
        self.blockdevice = blockdevice


DATASET = Field(
    u"dataset",
    lambda dataset: dataset.dataset_id,
    u"The unique identifier of a dataset."
)

VOLUME = Field(
    u"volume",
    lambda volume: volume.blockdevice_id,
    u"The unique identifier of a volume."
)

FILESYSTEM_TYPE = Field.forTypes(
    u"filesystem_type",
    [unicode],
    u"The name of a filesystem."
)

DATASET_ID = Field(
    u"dataset_id",
    lambda dataset_id: unicode(dataset_id),
    u"The unique identifier of a dataset."
)

MOUNTPOINT = Field(
    u"mountpoint",
    lambda path: path.path,
    u"The absolute path to the location on the node where the dataset will be "
    u"mounted.",
)

BLOCK_DEVICE_ID = Field(
    u"block_device_id",
    lambda id: unicode(id),
    u"The unique identifier if the underlying block device."
)

BLOCK_DEVICE_SIZE = Field(
    u"block_device_size",
    identity,
    u"The size of the underlying block device."
)

BLOCK_DEVICE_COMPUTE_INSTANCE_ID = Field(
    u"block_device_compute_instance_id",
    identity,
    u"An identifier for the host to which the underlying block device is "
    u"attached.",
)

BLOCK_DEVICE_PATH = Field(
    u"block_device_path",
    lambda path: path.path,
    u"The system device file for an attached block device."
)

PROFILE_NAME = Field.forTypes(
    u"profile_name",
    [unicode],
    u"The name of a profile for a volume."
)

CREATE_BLOCK_DEVICE_DATASET = ActionType(
    u"agent:blockdevice:create",
    [DATASET, MOUNTPOINT],
    [],
    u"A block-device-backed dataset is being created.",
)

# Really this is the successful completion of CREATE_BLOCK_DEVICE_DATASET.  It
# might be nice if these fields could just be added to the running action
# instead of being logged as a separate message (but still in the correct
# context).  Or maybe this is fine as-is.
BLOCK_DEVICE_DATASET_CREATED = MessageType(
    u"agent:blockdevice:created",
    [BLOCK_DEVICE_PATH, BLOCK_DEVICE_ID, DATASET_ID, BLOCK_DEVICE_SIZE,
     BLOCK_DEVICE_COMPUTE_INSTANCE_ID],
    u"A block-device-backed dataset has been created.",
)

DESTROY_BLOCK_DEVICE_DATASET = ActionType(
    u"agent:blockdevice:destroy",
    [DATASET_ID],
    [],
    u"A block-device-backed dataset is being destroyed.",
)

UNMOUNT_BLOCK_DEVICE = ActionType(
    u"agent:blockdevice:unmount",
    [DATASET_ID],
    [],
    u"A block-device-backed dataset is being unmounted.",
)

UNMOUNT_BLOCK_DEVICE_DETAILS = MessageType(
    u"agent:blockdevice:unmount:details",
    [BLOCK_DEVICE_ID, BLOCK_DEVICE_PATH],
    u"The device file for a block-device-backed dataset has been discovered."
)

MOUNT_BLOCK_DEVICE = ActionType(
    u"agent:blockdevice:mount",
    [DATASET_ID, BLOCK_DEVICE_ID],
    [],
    u"A block-device-backed dataset is being mounted.",
)

MOUNT_BLOCK_DEVICE_DETAILS = MessageType(
    u"agent:blockdevice:mount:details",
    [BLOCK_DEVICE_PATH],
    u"The device file for a block-device-backed dataset has been discovered."
)

ATTACH_VOLUME = ActionType(
    u"agent:blockdevice:attach_volume",
    [DATASET_ID, BLOCK_DEVICE_ID],
    [],
    u"The volume for a block-device-backed dataset is being attached."
)

DETACH_VOLUME = ActionType(
    u"agent:blockdevice:detach_volume",
    [DATASET_ID, BLOCK_DEVICE_ID],
    [],
    u"The volume for a block-device-backed dataset is being detached."
)

DESTROY_VOLUME = ActionType(
    u"agent:blockdevice:destroy_volume",
    [BLOCK_DEVICE_ID],
    [],
    u"The volume for a block-device-backed dataset is being destroyed."
)

CREATE_FILESYSTEM = ActionType(
    u"agent:blockdevice:create_filesystem",
    [BLOCK_DEVICE_PATH, FILESYSTEM_TYPE],
    [],
    u"A block device is being initialized with a filesystem.",
)

INVALID_DEVICE_PATH_VALUE = Field(
    u"invalid_value",
    lambda value: safe_repr(value),
    u"A value returned from IBlockDeviceAPI.get_device_path which could not "
    u"possibly be correct.  This likely indicates a bug in the "
    "IBlockDeviceAPI implementation.",
)

INVALID_DEVICE_PATH = MessageType(
    u"agent:blockdevice:discover_state:invalid_device_path",
    [DATASET_ID, INVALID_DEVICE_PATH_VALUE],
    u"The device path given by the IBlockDeviceAPI implementation was "
    u"invalid.",
)

CREATE_VOLUME_PROFILE_DROPPED = MessageType(
    u"agent:blockdevice:profiles:create_volume_with_profiles:profile_dropped",
    [DATASET_ID, PROFILE_NAME],
    u"The profile of a volume was dropped during creation because the backend "
    u"does not support profiles. Use a backend that provides "
    u"IProfiledBlockDeviceAPI to get profile support."
)

DISCOVERED_RAW_STATE = MessageType(
    u"agent:blockdevice:raw_state",
    [Field(u"raw_state", safe_repr)],
    u"The discovered raw state of the node's block device volumes.")


def _volume_field():
    """
    Create and return a ``PRecord`` ``field`` to hold a ``BlockDeviceVolume``.
    """
    return field(
        type=BlockDeviceVolume, mandatory=True,
        # Disable the automatic PRecord.create factory.  Callers can just
        # supply the right type, we don't need the magic coercion behavior
        # supplied by default.
        factory=lambda x: x
    )


class BlockDeviceVolume(PClass):
    """
    A block device that may be attached to a host.

    :ivar unicode blockdevice_id: An identifier for the block device which is
        unique across the entire cluster.  For example, an EBS volume
        identifier (``vol-4282672b``).  This is used to address the block
        device for operations like attach and detach.
    :ivar int size: The size, in bytes, of the block device.
    :ivar unicode attached_to: An opaque identifier for the node to which the
        volume is attached or ``None`` if it is currently unattached.  The
        identifier is supplied by the ``IBlockDeviceAPI.compute_instance_id``
        method based on the underlying infrastructure services (for example, if
        the cluster runs on AWS, this is very likely an EC2 instance id).
    :ivar UUID dataset_id: The Flocker dataset ID associated with this volume.
    """
    blockdevice_id = field(type=unicode, mandatory=True)
    size = field(type=int, mandatory=True)
    attached_to = field(
        type=(unicode, type(None)), initial=None, mandatory=True
    )
    dataset_id = field(type=UUID, mandatory=True)


def _blockdevice_volume_from_datasetid(volumes, dataset_id):
    """
    A helper to get the volume for a given dataset_id.

    :param list volumes: The ``BlockDeviceVolume`` instances to inspect for a
        match.
    :param UUID dataset_id: The identifier of the dataset the volume of which
        to find.

    :return: Either a ``BlockDeviceVolume`` matching the given ``dataset_id``
        or ``None`` if no such volume can be found.
    """
    for volume in volumes:
        if volume.dataset_id == dataset_id:
            return volume


# Get rid of this in favor of calculating each individual operation in
# BlockDeviceDeployer.calculate_changes.  FLOC-1772
@implementer(IStateChange)
class DestroyBlockDeviceDataset(PRecord):
    """
    Destroy the volume for a dataset with a primary manifestation on the node
    where this state change runs.

    :ivar UUID dataset_id: The unique identifier of the dataset to which the
        volume to be destroyed belongs.
    :ivar unicode blockdevice_id: The unique identifier of the
        ``IBlockDeviceAPI``-managed volume to be destroyed.
    """
    dataset_id = field(type=UUID, mandatory=True)
    blockdevice_id = field(type=unicode, mandatory=True)

    @classmethod
    def from_dataset(cls, discovered_dataset, desired_dataset):
        return cls(
            dataset_id=desired_dataset.dataset_id,
            blockdevice_id=discovered_dataset.blockdevice_id,
        )

    # This can be replaced with a regular attribute when the `_logger` argument
    # is no longer required by Eliot.
    @property
    def eliot_action(self):
        return DESTROY_BLOCK_DEVICE_DATASET(
            _logger, dataset_id=self.dataset_id
        )

    def run(self, deployer):
        return run_state_change(
            sequentially(
                changes=[
                    UnmountBlockDevice(dataset_id=self.dataset_id,
                                       blockdevice_id=self.blockdevice_id),
                    DetachVolume(dataset_id=self.dataset_id,
                                 blockdevice_id=self.blockdevice_id),
                    DestroyVolume(blockdevice_id=self.blockdevice_id),
                ]
            ),
            deployer,
        )


@implementer(IStateChange)
class CreateFilesystem(PRecord):
    """
    Create a filesystem on a block device.

    :ivar FilePath device: The device on which to create the filesystem.
    :ivar unicode filesystem: The name of the filesystem type to create.  For
        example, ``u"ext4"``.
    """
    device = field(type=FilePath, mandatory=True)
    filesystem = field(type=unicode, mandatory=True)

    @property
    def eliot_action(self):
        return CREATE_FILESYSTEM(
            _logger, block_device_path=self.device,
            filesystem_type=self.filesystem
        )

    def run(self, deployer):
        try:
            _ensure_no_filesystem(self.device)
            check_output([
                b"mkfs", b"-t", self.filesystem.encode("ascii"),
                # This is ext4 specific, and ensures mke2fs doesn't ask
                # user interactively about whether they really meant to
                # format whole device rather than partition. It will be
                # removed once upstream bug is fixed. See FLOC-2085.
                b"-F",
                self.device.path
            ])
        except:
            return fail()
        return succeed(None)


def _has_filesystem(device):
    """
    :return: Boolean which is true if a filesystem exists on the
        ``device``, otherwise false.
    """
    try:
        check_output(
            [b"blkid", b"-p", b"-u", b"filesystem", device.path],
            stderr=STDOUT,
        )
    except CalledProcessError as e:
        # According to the man page:
        #   the specified token was not found, or no (specified) devices
        #   could be identified
        #
        # Experimentation shows that there is no output in the case of the
        # former, and an error printed to stderr in the case of the
        # latter.
        #
        # FLOC-2388: We're assuming an interface. We should test this
        # assumption.
        if e.returncode == 2 and not e.output:
            # There is no filesystem on this device.
            return False
        raise
    return True


def _ensure_no_filesystem(device):
    """
    Raises an error if there's already a filesystem on ``device``.

    :raises: ``FilesystemExists`` if there is already a filesystem on
        ``device``.
    :return: ``None``
    """
    if _has_filesystem(device):
        raise FilesystemExists(device)


def _valid_size(size):
    """
    Pyrsistent invariant for filesystem size, which must be a multiple of 1024
    bytes.
    """
    if size % 1024 == 0:
        return (True, "")
    return (
        False, "Filesystem size must be multiple of 1024, not %d" % (size,)
    )


@implementer(IStateChange)
class MountBlockDevice(PRecord):
    """
    Mount the filesystem mounted from the block device backed by a particular
    volume.

    :ivar UUID dataset_id: The unique identifier of the dataset associated with
        the filesystem to mount.
    :ivar unicode blockdevice_id: The unique identifier of the
        ``IBlockDeviceAPI``-managed volume to be mounted.
    :ivar FilePath mountpoint: The filesystem location at which to mount the
        volume's filesystem.  If this does not exist, it is created.
    """
    dataset_id = field(type=UUID, mandatory=True)
    blockdevice_id = field(type=unicode, mandatory=True)
    mountpoint = field(type=FilePath, mandatory=True)

    @classmethod
    def from_dataset(cls, discovered_dataset, desired_dataset):
        return cls(
            dataset_id=desired_dataset.dataset_id,
            blockdevice_id=discovered_dataset.blockdevice_id,
            mountpoint=desired_dataset.mount_point,
        )

    @property
    def eliot_action(self):
        return MOUNT_BLOCK_DEVICE(_logger, dataset_id=self.dataset_id,
                                  block_device_id=self.blockdevice_id)

    def run(self, deployer):
        """
        Run the system ``mount`` tool to mount this change's volume's block
        device.  The volume must be attached to this node.
        """
        api = deployer.block_device_api
        device = api.get_device_path(self.blockdevice_id)
        MOUNT_BLOCK_DEVICE_DETAILS(block_device_path=device).write(_logger)

        # Create the directory where a device will be mounted.
        # The directory's parent's permissions will be set to only allow access
        # by owner, to limit access by other users on the node.
        try:
            self.mountpoint.makedirs()
        except OSError as e:
            if e.errno != EEXIST:
                return fail()
        self.mountpoint.parent().chmod(S_IRWXU)

        # This should be asynchronous.  FLOC-1797
        check_output([b"mount", device.path, self.mountpoint.path])

        # Remove lost+found to ensure filesystems always start out empty.
        # Mounted filesystem is also made world
        # writeable/readable/executable since we can't predict what user a
        # container will run as.  We make sure we change mounted
        # filesystem's root directory permissions, so we only do this
        # after the filesystem is mounted.  If other files exist we don't
        # bother with either change, since at that point user has modified
        # the volume and we don't want to undo their changes by mistake
        # (e.g. postgres doesn't like world-writeable directories once
        # it's initialized).

        # A better way is described in
        # https://clusterhq.atlassian.net/browse/FLOC-2074
        lostfound = self.mountpoint.child(b"lost+found")
        if self.mountpoint.children() == [lostfound]:
            lostfound.remove()
            self.mountpoint.chmod(S_IRWXU | S_IRWXG | S_IRWXO)
            self.mountpoint.restat()

        return succeed(None)


@implementer(IStateChange)
class UnmountBlockDevice(PRecord):
    """
    Unmount the filesystem mounted from the block device backed by a particular
    volume.

    :ivar UUID dataset_id: The unique identifier of the dataset associated with
        the filesystem to unmount.
    :ivar unicode blockdevice_id: The unique identifier of the mounted
        ``IBlockDeviceAPI``-managed volume to be unmounted.
    """
    dataset_id = field(type=UUID, mandatory=True)
    blockdevice_id = field(type=unicode, mandatory=True)

    @classmethod
    def from_dataset(cls, discovered_dataset, desired_dataset):
        return cls(
            dataset_id=discovered_dataset.dataset_id,
            blockdevice_id=discovered_dataset.blockdevice_id,
        )

    @property
    def eliot_action(self):
        return UNMOUNT_BLOCK_DEVICE(_logger, dataset_id=self.dataset_id)

    def run(self, deployer):
        """
        Run the system ``unmount`` tool to unmount this change's volume's block
        device.  The volume must be attached to this node and the corresponding
        block device mounted.
        """
        api = deployer.async_block_device_api
        deferred_device_path = api.get_device_path(self.blockdevice_id)

        def got_device(device):
            UNMOUNT_BLOCK_DEVICE_DETAILS(
                block_device_id=self.blockdevice_id,
                block_device_path=device
            ).write(_logger)
            # This should be asynchronous. FLOC-1797
            check_output([b"umount", device.path])
        deferred_device_path.addCallback(got_device)
        return deferred_device_path


@implementer(IStateChange)
class AttachVolume(PRecord):
    """
    Attach an unattached volume to this node (the node of the deployer it is
    run with).

    :ivar UUID dataset_id: The unique identifier of the dataset associated with
        the volume to attach.
    :ivar unicode blockdevice_id: The unique identifier of the
        ``IBlockDeviceAPI``-managed volume to be attached.
    """
    dataset_id = field(type=UUID, mandatory=True)
    blockdevice_id = field(type=unicode, mandatory=True)

    @classmethod
    def from_dataset(cls, discovered_dataset, desired_dataset):
        return cls(
            dataset_id=discovered_dataset.dataset_id,
            blockdevice_id=discovered_dataset.blockdevice_id,
        )

    @property
    def eliot_action(self):
        return ATTACH_VOLUME(_logger, dataset_id=self.dataset_id,
                             blockdevice_id=self.blockdevice_id)

    def run(self, deployer):
        """
        Use the deployer's ``IBlockDeviceAPI`` to attach the volume.
        """
        api = deployer.async_block_device_api
        getting_id = api.compute_instance_id()

        def got_compute_id(compute_instance_id):
            return api.attach_volume(
                self.blockdevice_id,
                attach_to=compute_instance_id,
            )
        attaching = getting_id.addCallback(got_compute_id)
        return attaching


@implementer(IStateChange)
class ActionNeeded(PClass):
    """
    We need to take some action on a dataset but lack the necessary
    information to do so.

    Creating this is still useful insofar as it may let the convergence
    loop know that it should wake up and do discovery, which would allow
    us to get the actual ``IStateChange`` calculated.

    :ivar UUID dataset_id: The unique identifier of the dataset associated with
        the volume to attach.
    """
    dataset_id = field(type=UUID, mandatory=True)

    # Nominal interface compliance; we don't expect this to be ever run,
    # it's just a marker object basically.
    eliot_action = None

    def run(self, deployer):
        """
        This should not ever be run; doing so suggests a bug somewhere.
        """
        return fail(NotImplementedError(
            "This should never happen when calculating in anger."))


@implementer(IStateChange)
class DetachVolume(PRecord):
    """
    Detach a volume from the node it is currently attached to.

    :ivar UUID dataset_id: The unique identifier of the dataset associated with
        the volume to detach.
    :ivar unicode blockdevice_id: The unique identifier of the
        ``IBlockDeviceAPI``-managed volume to be detached.
    """
    dataset_id = field(type=UUID, mandatory=True)
    blockdevice_id = field(type=unicode, mandatory=True)

    @classmethod
    def from_dataset(cls, discovered_dataset, desired_dataset):
        return cls(
            dataset_id=discovered_dataset.dataset_id,
            blockdevice_id=discovered_dataset.blockdevice_id,
        )

    @property
    def eliot_action(self):
        return DETACH_VOLUME(_logger, dataset_id=self.dataset_id,
                             block_device_id=self.blockdevice_id)

    def run(self, deployer):
        """
        Use the deployer's ``IBlockDeviceAPI`` to detach the volume.
        """
        api = deployer.async_block_device_api
        return api.detach_volume(self.blockdevice_id)


@implementer(IStateChange)
class DestroyVolume(PRecord):
    """
    Destroy the storage (and therefore contents) of a volume.

    :ivar unicode blockdevice_id: The unique identifier of the
        ``IBlockDeviceAPI``-managed volume to be destroyed.
    """
    blockdevice_id = field(type=unicode, mandatory=True)

    @classmethod
    def from_dataset(cls, discovered_dataset, desired_dataset):
        return cls(
            blockdevice_id=discovered_dataset.blockdevice_id,
        )

    @property
    def eliot_action(self):
        return DESTROY_VOLUME(_logger, block_device_id=self.blockdevice_id)

    def run(self, deployer):
        """
        Use the deployer's ``IBlockDeviceAPI`` to destroy the volume.
        """
        api = deployer.async_block_device_api
        return api.destroy_volume(self.blockdevice_id)


def allocated_size(allocation_unit, requested_size):
    """
    Round ``requested_size`` up to the nearest ``allocation_unit``.

    :param int allocation_unit: The interval in ``bytes`` to which
        ``requested_size`` will be rounded up.
    :param int requested_size: The size in ``bytes`` that is required.
    :return: The ``allocated_size`` in ``bytes``.
    """
    allocation_unit = int(allocation_unit)
    requested_size = int(requested_size)

    previous_interval_size = (
        (requested_size // allocation_unit)
        * allocation_unit
    )
    if previous_interval_size < requested_size:
        return previous_interval_size + allocation_unit
    else:
        return requested_size


# Get rid of this in favor of calculating each individual operation in
# BlockDeviceDeployer.calculate_changes. Also consider splitting the
# CreateVolume portion of the IStateChange into 2 IStateChanges, one for
# creating a volume with a profile, and one for creating a volume without a
# profile. FLOC-1771
@implementer(IStateChange)
class CreateBlockDeviceDataset(PRecord):
    """
    An operation to create a new dataset on a newly created volume with a newly
    initialized filesystem.

    :ivar Dataset dataset: The dataset for which to create a block device.
    :ivar FilePath mountpoint: The path at which to mount the created device.
    """
    dataset = field(mandatory=True, type=Dataset)
    mountpoint = field(mandatory=True, type=FilePath)

    @classmethod
    def from_dataset(cls, discovered_dataset, desired_dataset):
        return cls(
            dataset=Dataset(
                dataset_id=desired_dataset.dataset_id,
                maximum_size=desired_dataset.maximum_size,
            ),
            mountpoint=desired_dataset.mount_point,
        )

    @property
    def eliot_action(self):
        return CREATE_BLOCK_DEVICE_DATASET(
            _logger,
            dataset=self.dataset, mountpoint=self.mountpoint
        )

    def _create_volume(self, deployer):
        """
        Create the volume using the backend API. This method will create the
        volume with a profile if the metadata on the volume suggests that we
        should

        :param deployer: The deployer to use to create the volume.

        :returns: The created ``BlockDeviceVolume``.
        """
        api = deployer.block_device_api
        profile_name = self.dataset.metadata.get(PROFILE_METADATA_KEY)
        dataset_id = UUID(self.dataset.dataset_id)
        size = allocated_size(allocation_unit=api.allocation_unit(),
                              requested_size=self.dataset.maximum_size)
        if profile_name:
            return (
                deployer.profiled_blockdevice_api.create_volume_with_profile(
                    dataset_id=dataset_id,
                    size=size,
                    profile_name=profile_name
                )
            )
        else:
            return api.create_volume(dataset_id=dataset_id, size=size)

    def run(self, deployer):
        """
        Create a block device, attach it to the local host, create an ``ext4``
        filesystem on the device and mount it.

        Operations are performed synchronously.

        See ``IStateChange.run`` for general argument and return type
        documentation.

        :returns: An already fired ``Deferred`` with result ``None`` or a
            failed ``Deferred`` with a ``DatasetExists`` exception if a
            blockdevice with the required dataset_id already exists.
        """
        api = deployer.block_device_api
        try:
            check_for_existing_dataset(api, UUID(hex=self.dataset.dataset_id))
        except:
            return fail()

        volume = self._create_volume(deployer)

        # This duplicates AttachVolume now.
        volume = api.attach_volume(
            volume.blockdevice_id,
            attach_to=api.compute_instance_id(),
        )
        device = api.get_device_path(volume.blockdevice_id)

        create = CreateFilesystem(device=device, filesystem=u"ext4")
        d = run_state_change(create, deployer)

        mount = MountBlockDevice(dataset_id=volume.dataset_id,
                                 blockdevice_id=volume.blockdevice_id,
                                 mountpoint=self.mountpoint)
        d.addCallback(lambda _: run_state_change(mount, deployer))

        def passthrough(result):
            BLOCK_DEVICE_DATASET_CREATED(
                block_device_path=device,
                block_device_id=volume.blockdevice_id,
                dataset_id=volume.dataset_id,
                block_device_size=volume.size,
                block_device_compute_instance_id=volume.attached_to,
            ).write(_logger)
            return result
        d.addCallback(passthrough)
        return d


class IBlockDeviceAsyncAPI(Interface):
    """
    Common operations provided by all block device backends, exposed via
    asynchronous methods.
    """
    def allocation_unit():
        """
        See ``IBlockDeviceAPI.allocation_unit``.

        :returns: A ``Deferred`` that fires with ``int`` size of the
            allocation_unit.
        """

    def compute_instance_id():
        """
        See ``IBlockDeviceAPI.compute_instance_id``.

        :returns: A ``Deferred`` that fires with ``unicode`` of a
            provider-specific node identifier which identifies the node where
            the method is run, or fails with ``UnknownInstanceID`` if we cannot
            determine the identifier.
        """

    def create_volume(dataset_id, size):
        """
        See ``IBlockDeviceAPI.create_volume``.

        :returns: A ``Deferred`` that fires with a ``BlockDeviceVolume`` when
            the volume has been created.
        """

    def destroy_volume(blockdevice_id):
        """
        See ``IBlockDeviceAPI.destroy_volume``.

        :return: A ``Deferred`` that fires when the volume has been destroyed.
        """

    def attach_volume(blockdevice_id, attach_to):
        """
        See ``IBlockDeviceAPI.attach_volume``.

        :returns: A ``Deferred`` that fires with a ``BlockDeviceVolume`` with a
            ``attached_to`` attribute set to ``attach_to``.
        """

    def detach_volume(blockdevice_id):
        """
        See ``BlockDeviceAPI.detach_volume``.

        :returns: A ``Deferred`` that fires when the volume has been detached.
        """

    def list_volumes():
        """
        See ``BlockDeviceAPI.list_volume``.

        :returns: A ``Deferred`` that fires with a ``list`` of
            ``BlockDeviceVolume``\ s.
        """

    def get_device_path(blockdevice_id):
        """
        See ``BlockDeviceAPI.get_device_path``.

        :returns: A ``Deferred`` that fires with a ``FilePath`` for the device.
        """


class IBlockDeviceAPI(Interface):
    """
    Common operations provided by all block device backends, exposed via
    synchronous methods.

    Note: This is an early sketch of the interface and it'll be refined as we
    real blockdevice providers are implemented.
    """
    def allocation_unit():
        """
        The size, in bytes up to which ``IDeployer`` will round volume
        sizes before calling ``IBlockDeviceAPI.create_volume``.

        :rtype: ``int``
        """

    def compute_instance_id():
        """
        Get an identifier for this node.

        This will be compared against ``BlockDeviceVolume.attached_to``
        to determine which volumes are locally attached and it will be used
        with ``attach_volume`` to locally attach volumes.

        :raise UnknownInstanceID: If we cannot determine the identifier of the
            node.
        :returns: A ``unicode`` object giving a provider-specific node
            identifier which identifies the node where the method is run.
        """

    def create_volume(dataset_id, size):
        """
        Create a new volume.

        When called by ``IDeployer``, the supplied size will be
        rounded up to the nearest
        ``IBlockDeviceAPI.allocation_unit()``

        If the backend supports human-facing volumes names (i.e. names
        that show up in management UIs) then it is recommended that the
        newly created volume should be given a name that contains the
        dataset_id in order to ease debugging. Some implementations may
        choose not to do so or may not be able to do so.

        :param UUID dataset_id: The Flocker dataset ID of the dataset on this
            volume.
        :param int size: The size of the new volume in bytes.
        :returns: A ``BlockDeviceVolume``.
        """

    def destroy_volume(blockdevice_id):
        """
        Destroy an existing volume.

        :param unicode blockdevice_id: The unique identifier for the volume to
            destroy.

        :raises UnknownVolume: If the supplied ``blockdevice_id`` does not
            exist.

        :return: ``None``
        """

    def attach_volume(blockdevice_id, attach_to):
        """
        Attach ``blockdevice_id`` to the node indicated by ``attach_to``.

        :param unicode blockdevice_id: The unique identifier for the block
            device being attached.
        :param unicode attach_to: An identifier like the one returned by the
            ``compute_instance_id`` method indicating the node to which to
            attach the volume.

        :raises UnknownVolume: If the supplied ``blockdevice_id`` does not
            exist.
        :raises AlreadyAttachedVolume: If the supplied ``blockdevice_id`` is
            already attached.

        :returns: A ``BlockDeviceVolume`` with a ``attached_to`` attribute set
            to ``attach_to``.
        """

    def detach_volume(blockdevice_id):
        """
        Detach ``blockdevice_id`` from whatever host it is attached to.

        :param unicode blockdevice_id: The unique identifier for the block
            device being detached.

        :raises UnknownVolume: If the supplied ``blockdevice_id`` does not
            exist.
        :raises UnattachedVolume: If the supplied ``blockdevice_id`` is
            not attached to anything.
        :returns: ``None``
        """

    def list_volumes():
        """
        List all the block devices available via the back end API.

        :returns: A ``list`` of ``BlockDeviceVolume``s.
        """

    def get_device_path(blockdevice_id):
        """
        Return the device path that has been allocated to the block device on
        the host to which it is currently attached.

        :param unicode blockdevice_id: The unique identifier for the block
            device.
        :raises UnknownVolume: If the supplied ``blockdevice_id`` does not
            exist.
        :raises UnattachedVolume: If the supplied ``blockdevice_id`` is
            not attached to a host.
        :returns: A ``FilePath`` for the device.
        """


class MandatoryProfiles(Values):
    """
    Mandatory Storage Profiles to be implemented by ``IProfiledBlockDeviceAPI``
    implementers.  These will have driver-specific meaning, with the following
    desired meaning:

    :ivar GOLD: The profile for fast storage.
    :ivar SILVER: The profile for intermediate/default storage.
    :ivar BRONZE: The profile for cheap storage.
    :ivar DEFAULT: The default profile if none is specified.
    """
    GOLD = ValueConstant(u'gold')
    SILVER = ValueConstant(u'silver')
    BRONZE = ValueConstant(u'bronze')
    DEFAULT = ValueConstant(BRONZE.value)


class IProfiledBlockDeviceAPI(Interface):
    """
    An interface for drivers that are capable of creating volumes with a
    specific profile.
    """

    def create_volume_with_profile(dataset_id, size, profile_name):
        """
        Create a new volume with the specified profile.

        When called by ``IDeployer``, the supplied size will be
        rounded up to the nearest ``IBlockDeviceAPI.allocation_unit()``.


        :param UUID dataset_id: The Flocker dataset ID of the dataset on this
            volume.
        :param int size: The size of the new volume in bytes.
        :param unicode profile_name: The name of the storage profile for this
            volume.

        :returns: A ``BlockDeviceVolume`` of the newly created volume.
        """


@implementer(IProfiledBlockDeviceAPI)
class ProfiledBlockDeviceAPIAdapter(PClass):
    """
    Adapter class to create ``IProfiledBlockDeviceAPI`` providers for
    ``IBlockDeviceAPI`` implementations that do not implement
    ``IProfiledBlockDeviceAPI``

    :ivar _blockdevice_api: The ``IBlockDeviceAPI`` provider to back the
        volume creation.
    """
    _blockdevice_api = field(
        mandatory=True,
        invariant=lambda i: (IBlockDeviceAPI.providedBy(i),
                             '_blockdevice_api must provide IBlockDeviceAPI'))

    def create_volume_with_profile(self, dataset_id, size, profile_name):
        """
        Reverts to constructing a volume with no profile. To be used with
        backends that do not implement ``IProfiledBlockDeviceAPI``, but do
        implement ``IBlockDeviceAPI``.
        """
        CREATE_VOLUME_PROFILE_DROPPED(dataset_id=dataset_id,
                                      profile_name=profile_name).write()
        return self._blockdevice_api.create_volume(dataset_id=dataset_id,
                                                   size=size)


@implementer(IBlockDeviceAsyncAPI)
@auto_threaded(IBlockDeviceAPI, "_reactor", "_sync", "_threadpool")
class _SyncToThreadedAsyncAPIAdapter(PRecord):
    """
    Adapt any ``IBlockDeviceAPI`` to ``IBlockDeviceAsyncAPI`` by running its
    methods in threads of a thread pool.
    """
    _reactor = field()
    _sync = field()
    _threadpool = field()


def check_for_existing_dataset(api, dataset_id):
    """
    :param IBlockDeviceAPI api: The ``api`` for listing the existing volumes.
    :param UUID dataset_id: The dataset_id to check for.

    :raises: ``DatasetExists`` if there is already a ``BlockDeviceVolume`` with
        the supplied ``dataset_id``.
    """
    volumes = api.list_volumes()
    for volume in volumes:
        if volume.dataset_id == dataset_id:
            raise DatasetExists(volume)


def get_blockdevice_volume(api, blockdevice_id):
    """
    Find a ``BlockDeviceVolume`` matching the given identifier.

    :param unicode blockdevice_id: The backend identifier of the volume to
        find.

    :raise UnknownVolume: If no volume with a matching identifier can be
        found.

    :return: The ``BlockDeviceVolume`` that matches.
    """
    for volume in api.list_volumes():
        if volume.blockdevice_id == blockdevice_id:
            return volume
    raise UnknownVolume(blockdevice_id)


def get_system_mounts():
    """
    Load information about mounted filesystems.

    :return: Mapping from block devices to mountpoints.
    :rtype: ``dict`` mapping ``FilePath`` to ``FilePath``
    """
    partitions = psutil.disk_partitions()
    return {
        FilePath(partition.device):
        FilePath(partition.mountpoint)
        for partition
        in partitions
    }


def _manifestation_from_volume(volume):
    """
    :param BlockDeviceVolume volume: The block device which has the
        manifestation of a dataset.
    :returns: A primary ``Manifestation`` of a ``Dataset`` with the same id as
        the supplied ``BlockDeviceVolume``.
    """
    dataset = Dataset(
        dataset_id=volume.dataset_id,
        maximum_size=volume.size,
    )
    return Manifestation(dataset=dataset, primary=True)


class RawState(PClass):
    """
    The raw state of a node.

    :param unicode compute_instance_id: The identifier for this node.
    :param volumes: List of all volumes in the cluster.
    :type volumes: ``pvector`` of ``BlockDeviceVolume``
    :param devices: Mapping from dataset UUID to block device path containing
        filesystem of that dataset, on this particular node.
    :type devices: ``pmap`` of ``UUID`` to ``FilePath``
    :param system_mounts: Mapping of block device path to mount point of all
        mounts on this particular node.
    :type system_mounts: ``pmap`` of ``FilePath`` to ``FilePath``.
    :param devices_with_filesystems: ``PSet`` of ``FilePath`` including
        those devices that have filesystems on this node.
    """
    compute_instance_id = field(unicode, mandatory=True)
    volumes = pvector_field(BlockDeviceVolume)
    devices = pmap_field(UUID, FilePath)
    system_mounts = pmap_field(FilePath, FilePath)
    devices_with_filesystems = pset_field(FilePath)


@implementer(ILocalState)
class BlockDeviceDeployerLocalState(PClass):
    """
    An ``ILocalState`` implementation for the ``BlockDeviceDeployer``.

    :ivar unicode hostname: The IP address of the node that has this is the
        state for.
    :ivar UUID node_uuid: The UUID of the node that this is the state for.
    :ivar datasets: The datasets discovered from this node.
    :ivar volumes: A ``PVector`` of ``BlockDeviceVolume`` instances for all
        volumes in the cluster that this node is aware of.
    """
    hostname = field(type=unicode, mandatory=True)
    node_uuid = field(type=UUID, mandatory=True)
    datasets = pmap_field(UUID, DiscoveredDataset)
    # XXX This should go away in FLOC-3386
    volumes = pvector_field(BlockDeviceVolume)

    def shared_state_changes(self):
        """
        Returns the NodeState and the NonManifestDatasets of the local state.
        These are the only parts of the state that need to be sent to the
        control service.
        """
        # XXX The structure of the shared state changes reflects the model
        # currently used by the control service. However, that model doesn't
        # seem to actually match what any consumer wants.
        manifestations = {}
        paths = {}
        devices = {}
        nonmanifest_datasets = {}

        for dataset in self.datasets.values():
            dataset_id = dataset.dataset_id
            if dataset.state == DatasetStates.MOUNTED:
                manifestations[unicode(dataset_id)] = Manifestation(
                    dataset=Dataset(
                        dataset_id=dataset_id,
                        maximum_size=dataset.maximum_size,
                    ),
                    primary=True,
                )
                paths[unicode(dataset_id)] = dataset.mount_point
            elif dataset.state in (
                DatasetStates.NON_MANIFEST, DatasetStates.ATTACHED,
                DatasetStates.ATTACHED_NO_FILESYSTEM,
            ):
                nonmanifest_datasets[unicode(dataset_id)] = Dataset(
                    dataset_id=dataset_id,
                    maximum_size=dataset.maximum_size,
                )
            if dataset.state in (
                DatasetStates.MOUNTED, DatasetStates.ATTACHED,
                DatasetStates.ATTACHED_NO_FILESYSTEM,
            ):
                devices[dataset_id] = dataset.device_path

        return (
            NodeState(
                uuid=self.node_uuid,
                hostname=self.hostname,
                manifestations=manifestations,
                paths=paths,
                devices=devices,
                applications=None,
            ),
            NonManifestDatasets(
                datasets=nonmanifest_datasets
            ),
        )


# Mapping from desired and discovered dataset state to
# IStateChange factory. (The factory is expected to take
# ``desired_dataset`` and ``discovered_dataset``.
DATASET_TRANSITIONS = {
    DatasetStates.MOUNTED: {
        DatasetStates.NON_EXISTENT: CreateBlockDeviceDataset.from_dataset,
        DatasetStates.ATTACHED_ELSEWHERE: lambda **kwargs: NoOp(),
        DatasetStates.ATTACHED_NO_FILESYSTEM: CreateFilesystem.from_dataset,
        DatasetStates.NON_MANIFEST: AttachVolume.from_dataset,
        DatasetStates.ATTACHED: MountBlockDevice.from_dataset,
    },
    DatasetStates.NON_MANIFEST: {
        DatasetStates.NON_EXISTENT: lambda **kwargs: NoOp(),
        DatasetStates.ATTACHED_ELSEWHERE: lambda **kwargs: NoOp(),
        DatasetStates.ATTACHED_NO_FILESYSTEM: DetachVolume.from_dataset,
        DatasetStates.ATTACHED: DetachVolume.from_dataset,
        DatasetStates.MOUNTED: UnmountBlockDevice.from_dataset,
    },
    DatasetStates.DELETED: {
        DatasetStates.ATTACHED_ELSEWHERE: lambda **kwargs: NoOp(),
        DatasetStates.NON_MANIFEST: lambda **kwargs: NoOp(),
        DatasetStates.ATTACHED_NO_FILESYSTEM: DetachVolume.from_dataset,
        DatasetStates.ATTACHED: lambda **kwargs: NoOp(),
        DatasetStates.MOUNTED: DestroyBlockDeviceDataset.from_dataset,
    },
}


@implementer(ICalculater)
class BlockDeviceCalculater(PClass):
    """
    XXX
    """
    def _calculate_dataset_change(self, discovered_dataset, desired_dataset):

        # If the configuration doesn't know about a dataset,
        # we detach it.
        desired_state = (desired_dataset.state
                         if desired_dataset is not None
                         else DatasetStates.NON_MANIFEST)
        # If we haven't discovered a dataset, then it is doesn't
        # exist.
        discovered_state = (discovered_dataset.state
                            if discovered_dataset is not None
                            else DatasetStates.NON_EXISTENT)
        if desired_state != discovered_state:
            return DATASET_TRANSITIONS[desired_state][discovered_state](
                discovered_dataset=discovered_dataset,
                desired_dataset=desired_dataset,
            )
        else:
            return NoOp()

    def calculate_changes_for_datasets(
        self, discovered_datasets, desired_datasets
    ):
        actions = []
        # If a dataset isn't in the configuration, we don't act on it.
        for dataset_id in set(discovered_datasets) | set(desired_datasets):
            desired_dataset = desired_datasets.get(dataset_id)
            discovered_dataset = discovered_datasets.get(dataset_id)
            actions.append(self._calculate_dataset_change(
                discovered_dataset=discovered_dataset,
                desired_dataset=desired_dataset,
            ))

        return in_parallel(changes=actions)


@implementer(IDeployer)
class BlockDeviceDeployer(PRecord):
    """
    An ``IDeployer`` that operates on ``IBlockDeviceAPI`` providers.

    :ivar unicode hostname: The IP address of the node that has this deployer.
    :ivar UUID node_uuid: The UUID of the node that has this deployer.
    :ivar IBlockDeviceAPI block_device_api: The block device API that will be
        called upon to perform block device operations.
    :ivar IProfiledBlockDeviceAPI _profiled_blockdevice_api: The block device
        API that will be called upon to perform block device operations with
        profiles.
    :ivar FilePath mountroot: The directory where block devices will be
        mounted.
    :ivar _async_block_device_api: An object to override the value of the
        ``async_block_device_api`` property.  Used by tests.  Should be
        ``None`` in real-world use.
    :ivar ICalculater calculater: The object to use to calculate dataset
        changes.
    """
    hostname = field(type=unicode, mandatory=True)
    node_uuid = field(type=UUID, mandatory=True)
    block_device_api = field(mandatory=True)
    _profiled_blockdevice_api = field(mandatory=True, initial=None)
    _async_block_device_api = field(mandatory=True, initial=None)
    mountroot = field(type=FilePath, initial=FilePath(b"/flocker"))
    poll_interval = timedelta(seconds=60.0)
    calculater = field(
        # XXX We should abstact this invariant out.
        invariant=lambda i: (ICalculater.providedBy(i),
                             "Must provide ICalculater"),
        mandatory=True,
        initial=BlockDeviceCalculater())

    @property
    def profiled_blockdevice_api(self):
        """
        Get an ``IProfiledBlockDeviceAPI`` provider which can create volumes
        configured based on pre-defined profiles. This will use the
        _profiled_blockdevice_api attribute, falling back to the
        block_device_api attributed and finally an adapter implementation
        around the block_device_api if neither of those provide the interface.
        """
        if IProfiledBlockDeviceAPI.providedBy(self._profiled_blockdevice_api):
            return self._profiled_blockdevice_api
        if IProfiledBlockDeviceAPI.providedBy(self.block_device_api):
            return self.block_device_api
        return ProfiledBlockDeviceAPIAdapter(
            _blockdevice_api=self.block_device_api
        )

    @property
    def async_block_device_api(self):
        """
        Get an ``IBlockDeviceAsyncAPI`` provider which can manipulate volumes
        for this deployer.

        During real operation, this is a threadpool-based wrapper around the
        ``IBlockDeviceAPI`` provider.  For testing purposes it can be
        overridden with a different object entirely (and this large amount of
        support code for this is necessary because this class is a ``PRecord``
        subclass).
        """
        if self._async_block_device_api is None:
            from twisted.internet import reactor
            return _SyncToThreadedAsyncAPIAdapter(
                _sync=self.block_device_api,
                _reactor=reactor,
                _threadpool=reactor.getThreadPool(),
            )
        return self._async_block_device_api

    def _discover_raw_state(self):
        """
        Find the state of this node that is relevant to determining which
        datasets are on this node, and return a ``RawState`` containing that
        information.
        """
        # FLOC-1819 Make this asynchronous
        api = self.block_device_api
        compute_instance_id = api.compute_instance_id()
        volumes = api.list_volumes()
        system_mounts = get_system_mounts()

        def is_existing_block_device(dataset_id, path):
            if isinstance(path, FilePath) and path.isBlockDevice():
                return True
            INVALID_DEVICE_PATH(
                dataset_id=dataset_id, invalid_value=path
            ).write(_logger)
            return False

        # XXX This should probably just be included in
        # BlockDeviceVolume for attached volumes.
        devices = {}
        for volume in volumes:
            dataset_id = volume.dataset_id
            if volume.attached_to == compute_instance_id:
                device_path = api.get_device_path(volume.blockdevice_id)
                if is_existing_block_device(dataset_id, device_path):
                    devices[dataset_id] = device_path
                else:
                    # XXX We will detect this as NON_MANIFEST, but this is
                    # probably an intermediate state where the device is
                    # externally attached but the device hasn't shown up
                    # in the filesystem yet.
                    pass

        result = RawState(
            compute_instance_id=compute_instance_id,
            volumes=volumes,
            devices=devices,
            system_mounts=system_mounts,
            devices_with_filesystems=[device for device in devices.values()
                                      if _has_filesystem(device)],
        )
        DISCOVERED_RAW_STATE(raw_state=result).write()
        return result

    def discover_state(self, node_state):
        """
        Find all datasets that are currently associated with this host and
        return a ``BlockDeviceDeployerLocalState`` containing all the datasets
        that are not manifest or are located on this node.
        """
        raw_state = self._discover_raw_state()

        datasets = {}
        for volume in raw_state.volumes:
            dataset_id = volume.dataset_id
            if dataset_id in raw_state.devices:
                device_path = raw_state.devices[dataset_id]
                mount_point = self._mountpath_for_dataset_id(
                    unicode(dataset_id)
                )
                if (
                    device_path in raw_state.system_mounts and
                    raw_state.system_mounts[device_path] == mount_point
                ):
                    datasets[dataset_id] = DiscoveredDataset(
                        state=DatasetStates.MOUNTED,
                        dataset_id=dataset_id,
                        maximum_size=volume.size,
                        blockdevice_id=volume.blockdevice_id,
                        device_path=device_path,
                        mount_point=mount_point,
                    )
                else:
                    if device_path in raw_state.devices_with_filesystems:
                        state = DatasetStates.ATTACHED
                    else:
                        state = DatasetStates.ATTACHED_NO_FILESYSTEM
                    datasets[dataset_id] = DiscoveredDataset(
                        state=state,
                        dataset_id=dataset_id,
                        maximum_size=volume.size,
                        blockdevice_id=volume.blockdevice_id,
                        device_path=device_path,
                    )
            else:
                if volume.attached_to in (None, raw_state.compute_instance_id):
                    # XXX We check for attached locally for the case
                    # where the volume is attached but the
                    # blockdevice doesn't exist yet.
                    datasets[dataset_id] = DiscoveredDataset(
                        state=DatasetStates.NON_MANIFEST,
                        dataset_id=dataset_id,
                        maximum_size=volume.size,
                        blockdevice_id=volume.blockdevice_id,
                    )
                else:
                    datasets[dataset_id] = DiscoveredDataset(
                        state=DatasetStates.ATTACHED_ELSEWHERE,
                        dataset_id=dataset_id,
                        maximum_size=volume.size,
                        blockdevice_id=volume.blockdevice_id,
                    )

        local_state = BlockDeviceDeployerLocalState(
            node_uuid=self.node_uuid,
            hostname=self.hostname,
            datasets=datasets,
            # XXX This should go away in FLOC-3386
            volumes=raw_state.volumes,
        )

        return succeed(local_state)

    def _mountpath_for_dataset_id(self, dataset_id):
        """
        Calculate the mountpoint for a dataset.

        :param unicode dataset_id: The unique identifier of the dataset for
            which to calculate a mount point.

        :returns: A ``FilePath`` of the mount point.
        """
        return self.mountroot.child(dataset_id.encode("ascii"))

    def _calculate_desired_state(
        self, configuration, local_applications, local_datasets
    ):
        not_in_use = NotInUseDatasets(
            node_uuid=self.node_uuid,
            applications=local_applications,
            leases=configuration.leases,
        )

        this_node_config = configuration.get_node(
            self.node_uuid, hostname=self.hostname)

        desired_datasets = {}
        for manifestation in this_node_config.manifestations.values():
            dataset_id = UUID(manifestation.dataset.dataset_id)
            # XXX: Make this configurable. FLOC-2679
            maximum_size = manifestation.dataset.maximum_size
            if maximum_size is None:
                maximum_size = DEFAULT_DATASET_SIZE

            if (
                manifestation.dataset.deleted is True
                # XXX FLOC-1772
                and dataset_id in local_datasets
                and local_datasets[dataset_id].state == DatasetStates.MOUNTED
            ):
                desired_datasets[dataset_id] = DesiredDataset(
                    state=DatasetStates.DELETED,
                    dataset_id=dataset_id,
                    metadata=manifestation.dataset.metadata,
                )
            else:
                desired_datasets[dataset_id] = DesiredDataset(
                    state=DatasetStates.MOUNTED,
                    dataset_id=dataset_id,
                    metadata=manifestation.dataset.metadata,
                    maximum_size=maximum_size,
                    mount_point=self._mountpath_for_dataset_id(
                        unicode(dataset_id)
                    ),
                )

        # If we don't have a given dataset, we default it to `NON_MANIFEST` in
        # BlockDeviceCalculater.calculate_changes_for_datasets, so we don't try
        # to find them here. We don't have explicit configuration for
        # non-manifest datasets anyway. Datasets that should be
        # `ATTACHED_ELSEWHERE` need the same behavior as `NON_MANIFEST`, so we
        # don't check them either.

        not_in_use_datasets = not_in_use(local_datasets.values())
        for dataset_id, dataset in local_datasets.items():
            if dataset in not_in_use_datasets:
                continue
            if dataset.state != DatasetStates.MOUNTED:
                # A lease doesn't force a mount.
                continue
            # This may override something from above, if there is a
            # lease or application using a dataset.
            desired_datasets[dataset_id] = DesiredDataset(
                dataset_id=dataset_id,
                state=DatasetStates.MOUNTED,
                maximum_size=dataset.maximum_size,
                # XXX We don't populate metadata here, but it isn't necessary
                # until we want to update it.
                metadata={},
                mount_point=self._mountpath_for_dataset_id(
                    unicode(dataset_id)
                ),
            )

        return desired_datasets

    def calculate_changes(self, configuration, cluster_state, local_state):
        local_node_state = cluster_state.get_node(self.node_uuid,
                                                  hostname=self.hostname)

        # We need to know applications (for now) to see if we should delay
        # deletion or handoffs. Eventually this will rely on leases instead.
        # https://clusterhq.atlassian.net/browse/FLOC-1425.
        if local_node_state.applications is None:
            return NoOp()

        desired_datasets = self._calculate_desired_state(
            configuration=configuration,
            local_applications=local_node_state.applications,
            local_datasets=local_state.datasets,
        )

        return self.calculater.calculate_changes_for_datasets(
            discovered_datasets=local_state.datasets,
            desired_datasets=desired_datasets,
        )


class ProcessLifetimeCache(proxyForInterface(IBlockDeviceAPI, "_api")):
    """
    A transparent caching layer around an ``IBlockDeviceAPI`` instance,
    intended to exist for the lifetime of the process.

    :ivar _api: Wrapped ``IBlockDeviceAPI`` provider.
    :ivar _instance_id: Cached result of ``compute_instance_id``.
    :ivar _device_paths: Mapping from blockdevice ids to cached device path.
    """
    def __init__(self, api):
        self._api = api
        self._instance_id = None
        self._device_paths = {}

    def compute_instance_id(self):
        """
        Always return initial result since this shouldn't change until a
        reboot.
        """
        if self._instance_id is None:
            self._instance_id = self._api.compute_instance_id()
        return self._instance_id

    def get_device_path(self, blockdevice_id):
        """
        Load the device path from a cache if possible.
        """
        if blockdevice_id not in self._device_paths:
            self._device_paths[blockdevice_id] = self._api.get_device_path(
                blockdevice_id)
        return self._device_paths[blockdevice_id]

    def detach_volume(self, blockdevice_id):
        """
        Clear the cached device path, if it was cached.
        """
        try:
            del self._device_paths[blockdevice_id]
        except KeyError:
            pass
        return self._api.detach_volume(blockdevice_id)
