.. role:: python(code)
  :language: python
  :class: highlight

===============================
Following on-the-fly processing
===============================

Following Relion processing on-the-fly as data is being collected is useful during data taking sessions 
that span days. At `eBIC <https://www.diamond.ac.uk/Instruments/Biological-Cryo-Imaging/eBIC.html>`_ 
key results from the Relion processing pipeline are stored in the `ISPyB database <https://ispyb.github.io/ISPyB/>`_ 
as soon as they become available. Systems are then needed to not only collect data from a Relion project but to 
maintain records of what data has been sent to the database and the various kinds of relationships between 
the database tables and how they correspond to the structure of the Relion pipeline. 

Relion itself keeps track of individual job's inputs and outputs in the form of a graph, the nodes and edges of 
which are recorded in the ``default_pipeline.star`` file. It is therefore natural to mimic this structure when extracting 
results from a project.

Nodes and graphs
================

:python:`relion.protonode` provides some basic functionality for setting up a data collection workflow. The base object 
is :python:`relion.protonode.protonode.ProtoNode` which has an associated environment, along with various hidden attributes. The 
latter include lists that hold any incoming and outgoing nodes and a record of which incoming nodes have been called etc. 
The ``environment`` behaves similarly to a dictionary and is intended to store any data needed by the node, either for 
processing or to pass onwards to other nodes. 

Environment
-----------

The environment itself contains a series of dictionaries which are searched in a specific order for the key provided. 
If a key is not found :python:`None` is returned, rather than raising a :python:`KeyError`. The dictionary hierarchy in 
``environment`` is:

* ``base``: searched first, updated with :python:`environment[key] = value`
* ``propagate.store``: accessed directly through :python:`environment.propagate[key]`, only accessed by :python:`environment[key]` 
if not empty
* ``escalate.store``: similar to ``propagate`` but rather than a dictionary is another node's ``environment`` allowing for a 
recursive search