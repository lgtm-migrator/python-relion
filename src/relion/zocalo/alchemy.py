from sqlalchemy import Column, Text
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
    prometheus_text = Column(
        Text,
        comment="Full prometheus format text with which to update http server",
    )


def buffer_url() -> str:
    import ispyb.sqlalchemy

    sqlalchemy_url = ispyb.sqlalchemy.url()
    local_url = "/".join(sqlalchemy_url.split("/")[:-1]) + "/zocalo"
    return local_url
