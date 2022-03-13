import time
import numpy as np
from datetime import datetime

from opcua import ua, Server

# INFO: The concept in this example is that the software model is first built in OPC UA via XML. After that, matching
# python objects are created based on the UA address space design. Do not use this example to build a UA address space
# from the python design.

# The advantage of this is that the software can be designed in a tool like UA Modeler. Then with minimal setup, a
# python program will import the XML and mirror all the objects in the software design. After this mirroring is achieved
# the user can focus on programming in python knowing that all all data from UA clients will reach the python object,
# and all data that needs to be output to the server can be published from the python object.
#
# Be aware that subscription calls are asynchronous.


class SubHandler(object):
    """
    Subscription Handler. To receive events from server for a subscription.
    The handler forwards updates to it's referenced python object
    """

    def __init__(self, obj):
        self.obj = obj

    def datachange_notification(self, node, val, data):
        # print("Python: New data change event", node, val, data)

        _node_name = node.get_browse_name()
        setattr(self.obj, _node_name.Name, data.monitored_item.Value.Value.Value)


class UaObject(object):
    """
    Python object which mirrors an OPC UA object
    Child UA variables/properties are auto subscribed to to synchronize python with UA server
    Python can write to children via write method, which will trigger an update for UA clients
    """
    def __init__(self, opcua_server, ua_node):
        self.opcua_server = opcua_server
        self.nodes = {}
        self.b_name = ua_node.get_browse_name().Name

        # keep track of the children of this object (in case python needs to write, or get more info from UA server)
        for _child in ua_node.get_children():
            _child_name = _child.get_browse_name()
            self.nodes[_child_name.Name] = _child

        # find all children which can be subscribed to (python object is kept up to date via subscription)
        sub_children = ua_node.get_properties()
        sub_children.extend(ua_node.get_variables())

        # subscribe to properties/variables
        handler = SubHandler(self)
        sub = opcua_server.create_subscription(500, handler)
        handle = sub.subscribe_data_change(sub_children)

    def write(self, attr=None):
        # if a specific attr isn't passed to write, write all OPC UA children
        if attr is None:
            for k, node in self.nodes.items():
                node_class = node.get_node_class()
                if node_class == ua.NodeClass.Variable:
                    node.set_value(getattr(self, k))
        # only update a specific attr
        else:
            self.nodes[attr].set_value(getattr(self, attr))


class MyObj(UaObject):
    """
    Definition of OPC UA object which represents a object to be mirrored in python
    This class mirrors it's UA counterpart and semi-configures itself according to the UA model (generally from XML)
    """
    def __init__(self, opcua_server, ua_node):

        # init the UaObject super class to connect the python object to the UA object
        super().__init__(opcua_server, ua_node)

        # local values only for use inside python
        self.testval = 'python only'

        # If the object has other objects as children it is best to search by type and instantiate more
        # mirrored python classes so that your UA object tree matches your python object tree

        # ADD CUSTOM OBJECT INITIALIZATION BELOW
        # find children by type and instantiate them as sub-objects of this class
        # NOT PART OF THIS EXAMPLE


if __name__ == "__main__":

    # setup our server
    server = Server()
    server.set_endpoint("opc.tcp://localhost:49320")
    server.set_server_name("Raman Spectrometer Simulation Server")

    # setup our own namespace, not really necessary but should as spec
    uri = "http://mynamespace"
    idx = server.register_namespace(uri)

    # get Objects node, this is where we should put our nodes
    objects = server.get_objects_node()
    

    # populating our address space; in most real use cases this should be imported from UA spec XML
    device = objects.add_object(idx, "Device")
    ch1 = device.add_folder(idx, "Channel1")
    spec1 = ch1.add_folder(idx, "Spectrum")
    intensity = spec1.add_variable(idx, "Intensity", 0.0)
    random_value = spec1.add_variable(idx, "random_value", 0.0)
    
    timestamp = spec1.add_variable(idx, "Timestamp", 0.0)

    ch2 = device.add_folder(idx, "Channel2")
    spec2 = ch2.add_folder(idx, "Spectrum")
    intensity = spec2.add_variable(idx, "Intensity", 0.0)
    timestamp = spec2.add_variable(idx, "Timestamp", 0.0)

    ch3 = device.add_folder(idx, "Channel3")
    spec3 = ch3.add_folder(idx, "Spectrum")
    intensity = spec3.add_variable(idx, "Intensity", 0.0)
    timestamp = spec3.add_variable(idx, "Timestamp", 0.0)

    ch4 = device.add_folder(idx, "Channel4")
    spec4 = ch4.add_folder(idx, "Spectrum")
    intensity = spec4.add_variable(idx, "Intensity", 0.0)
    timestamp = spec4.add_variable(idx, "Timestamp", 0.0)


    # starting!
    server.start()
    server.historize_node_data_change(random_value, period=None, count=100)
    # after the UA server is started initialize the mirrored object
    my_python_obj = MyObj(server, device)
    
    try:
        while True:

            # write directly to the OPC UA node of the object
            dv = ua.DataValue(ua.Variant(np.random.random(3325).tolist())) # replace with model data
            dv.SourceTimestamp = datetime.utcnow()
            my_python_obj.nodes["Channel1"].get_child("2:Spectrum").get_child("2:Intensity").set_data_value(dv)
            my_python_obj.nodes["Channel2"].get_child("2:Spectrum").get_child("2:Intensity").set_data_value(dv)
            my_python_obj.nodes["Channel3"].get_child("2:Spectrum").get_child("2:Intensity").set_data_value(dv)
            my_python_obj.nodes["Channel4"].get_child("2:Spectrum").get_child("2:Intensity").set_data_value(dv)
            my_python_obj.nodes["Channel1"].get_child("2:Spectrum").get_child("2:random_value").set_data_value(np.random.random())

            dt = ua.DataValue(ua.Variant(datetime.now().isoformat())) # replace with model data
            dt.SourceTimestamp = datetime.utcnow()
            my_python_obj.nodes["Channel1"].get_child("2:Spectrum").get_child("2:Timestamp").set_data_value(dt)
            my_python_obj.nodes["Channel2"].get_child("2:Spectrum").get_child("2:Timestamp").set_data_value(dt)
            my_python_obj.nodes["Channel3"].get_child("2:Spectrum").get_child("2:Timestamp").set_data_value(dt)
            my_python_obj.nodes["Channel4"].get_child("2:Spectrum").get_child("2:Timestamp").set_data_value(dt)


            time.sleep(1)

    finally:
        # close connection, remove subscriptions, etc
        server.stop()
