
"""libcloud driver for the Xen API

"""

from libcloud.types import Provider, NodeState, InvalidCredsError, MalformedResponseError, LibcloudError
from libcloud.base import ConnectionKey, Response
from libcloud.base import NodeDriver, NodeSize, Node, NodeLocation
from libcloud.base import NodeAuthPassword, NodeAuthSSHKey
from libcloud.base import NodeImage

from copy import copy
import json
import itertools
import os
import time

import XenAPI as api

import pprint

pprinter = pprint.PrettyPrinter(indent=2)
pp = pprinter.pprint


class XenException(Exception):
    """
    Error originating from the Xen API
    """
    def __str__(self):
        return "(%u) %s" % (self.args[0], self.args[1])
    def __repr__(self):
        return "<XenException code %u '%s'>" % (self.args[0], self.args[1])




class XenNodeDriver(NodeDriver):
    """
    libcloud driver for the Xen API
    """
    type = Provider.LINODE
    name = "Xen"

    session = None

    def __init__(self, key, secret=None, secure=True, host=None, port=None):
        """Instantiate the driver with the given API key

        @keyword key: the API key to use
        @type key: C{str}

        @keyword secret:

        @keyword secure:

        @keyword host:

        @keyword port:

        """
        if port is not None:
            host = "%s:%s" % (host, port)
        self.session = api.Session("http://%s/" % host)
        self.session.xenapi.login_with_password(key, secret)


    # Converts Xen VM 'power_state' to a NodeState constant.
    XEN_VM_STATES = {
        'Unknown': NodeState.UNKNOWN,              # Boot Failed
        'Running': NodeState.RUNNING,              # Running
        'Halted': NodeState.TERMINATED,           # Powered Off
        'Suspended': NodeState.TERMINATED,
        'Paused':NodeState.TERMINATED,
    }

    def list_nodes(self):
        """
        List all Xen VMs that are located on the given server

        @return: C{list} of L{Node} objects that the API key can access
        """

        all_records = self.session.xenapi.VM.get_all_records()

        vms_data = {}

        for (vm_ref, vm_info) in all_records.items():
            if vm_info['is_a_template'] or vm_info['is_a_snapshot']:
                # Skip templates and snapshots
                continue


            #vifs = vm_info['VIFs']
            #for vif_ref in vifs:
                #vif_info = self.session.xenapi.VIF.get_record(vif_ref)
                #print "VIF DATA:"
                #pp(vif_info)

                #vif_metrics = self.session.xenapi.VIF_metrics.get_record(vif_info['metrics'])
                #print "VIF METRICS:"
                #pp(vif_metrics)

                #vif_network = self.session.xenapi.network.get_record(vif_info['network'])
                #print "NETWORK:"
                #pp(vif_network)

            #vmm_ref = self.session.xenapi.VM.get_metrics(vm_ref)
            #vm_metrics = self.session.xenapi.VM_metrics.get_record(vmm_ref)
            #print "VM METRICS:"
            #pp(vm_metrics)

            gm_ref = self.session.xenapi.VM.get_guest_metrics(vm_ref)
            if "NULL" not in gm_ref:
                guest_metrics = self.session.xenapi.VM_guest_metrics.get_record(gm_ref)
                print "GUEST METRICS:"
                pp(guest_metrics)
                ip = guest_metrics['networks'].get('0/ip', None)
                print "IP ADDRESS: %s" % ip
                vm_info['ext_ip_address'] = ip

            vms_data[vm_ref] = vm_info

        return self._to_nodes(vms_data)

    def node_status(self, node=None, node_id=None):
        """
        Returns the node data without changing its status

        Accepts either a Node object or just a node_id, because it
        often is bloody annoying to build a full node even if what we need is
        an uid
        """
        node_id = node_id or node.id
        opaque_ref = self.session.xenapi.VM.get_by_uuid(node_id)
        data = self.session.xenapi.VM.get_record(opaque_ref)

        data = {opaque_ref: data}
        return self._to_nodes(data)[0]


    def start_node(self, node=None, node_id=None, wait=False):
        """
        Starts a node which is currently shot down

        Accepts either a Node object or just a node_id, because it
        often is bloody annoying to build a full node even if what we need is
        an uid

        @keyword node: the Node to booted. Can be None - node_id will then be used instead
        @type node: Node.

        @keyword node_id: the id of the node Node to be booted. Can be None, but then we need to provide the 'node' parameter
        @type node: int.

        @keyword wait: wait for the node to boot fully before returning
        @type wait: boolean

        Underlying API:
        VM.start

        """

        node_id = node_id or node.id
        opaque_ref = self.session.xenapi.VM.get_by_uuid(node_id)
        if wait:
            self.session.xenapi.VM.start(opaque_ref, False, True)
        else:
            self.session.xenapi.Async.VM.start(opaque_ref, False, True)
        data = self.session.xenapi.VM.get_record(opaque_ref)

        print "DATA IS: %s" % data

        data = {opaque_ref: data}
        return self._to_nodes(data)[0]




    def stop_node(self, node=None, node_id=None, wait=False):
        """
        Shuts down a node which is currently running

        @keyword node: the Node to be shut down
        @type node: Node
        @keyword wait: wait for the node to shut down fully before returning
        @type wait: boolean

        Underlying API:
        linode.shutdown()

        Issues a shutdown job for a given LinodeID.

        """

        node_id = node_id or node.id
        opaque_ref = self.session.xenapi.VM.get_by_uuid(node_id)
        if wait:
            self.session.xenapi.VM.clean_shutdown(opaque_ref)
        else:
            self.session.xenapi.Async.VM.clean_shutdown(opaque_ref)
        data = self.session.xenapi.VM.get_record(opaque_ref)

        print "DATA IS: %s" % data

        data = {opaque_ref: data}
        return self._to_nodes(data)[0]


    def reboot_node(self, node=None, node_id=None, wait=False):
        """Reboot the given Linode

        Will issue a shutdown job followed by a boot job, using the last booted
        configuration.  In most cases, this will be the only configuration.

        @keyword node: the Linode to reboot
        @type node: L{Node}"""

        node_id = node_id or node.id
        opaque_ref = self.session.xenapi.VM.get_by_uuid(node_id)
        if wait:
            self.session.xenapi.VM.clean_reboot(opaque_ref)
        else:
            self.session.xenapi.Async.VM.clean_reboot(opaque_ref)
        data = self.session.xenapi.VM.get_record(opaque_ref)

        print "DATA IS: %s" % data

        data = {opaque_ref: data}
        return self._to_nodes(data)[0]


    def destroy_node(self, node=None, node_id=None):
        """Destroy the given Xen VM

        TODO

        @keyword node: the Linode to destroy
        @type node: L{Node}"""

        # Linode expects a number. If it's not a number we let it fail
        node_id = int(node_id or node.id)

        params = { "api_action": "linode.delete", "LinodeID": node_id,
            "skipChecks": True }
        self.connection.request(LINODE_ROOT, params=params)
        return True

    def create_node(self, **kwargs):
        """
        Create a new Linode, deploy a Linux distribution, and boot

        TODO
        """
        pass


    def list_sizes(self, location=None):
        """
        Stub method which returns a single 'plan'

        Can be extended to provide a list of pre-set VM sizes

        """
        return [ NodeSize(id='default', name='Default Plan', ram="512",
                    disk="20000", bandwidth="100",
                    price="0.00", driver=self) ]


    def list_images(self):
        """
        List available Linux distributions

        Retrieve all Linux distributions that can be deployed.
        @return: a C{list} of L{NodeImage}s
        """

        all_records = self.session.xenapi.VM.get_all_records()

        templates = []
        for (vm_ref, vm_info) in all_records.items():
            if not vm_info['is_a_template']:
                continue

            i = NodeImage(id=vm_info['uuid'],
                          name=vm_info['name_label'],
                          driver=self,
                          extra={})
            templates.append(i)

        return templates

    def list_locations(self):
        """List available facilities for deployment

        @return: a C{list} with a single default L{NodeLocation} object"""
        return [NodeLocation("default",
                             "default",
                             "not specified",
                              self)]
        return nl


    def _to_nodes(self, data):
        """
        Convert a Xen response, which is a dict where keys are UIDs and
        values are dicts with host info

        @keyword data: C{dict} are returned by Xen API
        @type objs: C{dict}
        @return: C{list} of L{Node}s

        The method expects that templates and snapshots
        have been already filtered out
        """


        nodes = []

        for (opaque_ref, info) in data.items():
            #pp(info)

            node = Node(
                id =info['uuid'],
                name = info['name_label'],
                state = self.XEN_VM_STATES[info['power_state']], #TODO: Convert to libcloud const
                # This is not returned directly by Xen's VM.get_all_records
                # or VM.get_record calls but is added after querying VM's
                # 'guest properties'
                public_ip = info.get('ext_ip_address', None),
                private_ip = None, # TODO
                driver = self,
                extra = info
                )
            nodes.append(node)

        return nodes


    features = {"create_node": ["ssh_key", "password"]}
