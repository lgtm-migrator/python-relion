from sqlalchemy import TIMESTAMP, Column, String, Text, text
from sqlalchemy.dialects.mysql import INTEGER
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class ZocaloBuffer(Base):
    __tablename__ = "ZocaloBuffer"

    AutoProcProgramID = Column(
        INTEGER(10),
        primary_key=True,
        comment="Reference to an existing AutoProcProgram",
        autoincrement=False,
        nullable=False,
    )
    UUID = Column(
        INTEGER(10),
        primary_key=True,
        comment="AutoProcProgram-specific unique identifier",
        autoincrement=False,
        nullable=False,
    )
    Reference = Column(
        INTEGER(10),
        comment="Context-dependent reference to primary key IDs in other ISPyB tables",
    )


class ClusterMonitoring(Base):
    __tablename__ = "ClusterMonitoring"

    event_id = Column(
        INTEGER(10),
        primary_key=True,
        comment="Unique ID needed as cluster ID may not be unique with data from more than one cluster",
        autoincrement=True,
    )
    cluster_id = Column(
        INTEGER(10),
        primary_key=True,
        comment="ID of the cluster job",
        autoincrement=False,
        nullable=False,
    )
    AutoProcProgramID = Column(
        INTEGER(10),
        comment="Reference to the AutoProcProgram the cluster job is attached to",
        autoincrement=False,
    )
    command = Column(
        String(250),
        comment="Reference to the AutoProcProgram the cluster job is attached to",
    )
    event_time = Column(
        TIMESTAMP,
        nullable=False,
        index=True,
        server_default=text("current_timestamp() ON UPDATE current_timestamp()"),
        comment="Time of event",
    )
    event_type = Column(
        String(10),
        comment="Type of event being recorded. May be start, success or failure",
    )
    cluster = Column(
        String(250),
        comment="Name of the cluster the job ran on",
    )
    host_name = Column(
        String(250),
        comment="Name of the host the job ran on",
    )
    output_details = Column(Text, comment="Full output of wrapped command")


def buffer_url() -> str:
    import ispyb.sqlalchemy

    sqlalchemy_url = ispyb.sqlalchemy.url()
    local_url = "/".join(sqlalchemy_url.split("/")[:-1]) + "/zocalo"
    return local_url
