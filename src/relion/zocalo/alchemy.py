from sqlalchemy import TIMESTAMP, Column, ForeignKey, String
from sqlalchemy.dialects.mysql import INTEGER
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

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


class ClusterJobInfo(Base):
    __tablename__ = "ClusterJobInfo"

    cluster = Column(
        String(250),
        nullable=False,
        comment="Name of cluster",
        primary_key=True,
    )
    cluster_id = Column(
        INTEGER(10),
        primary_key=True,
        comment="ID of the cluster job",
        autoincrement=False,
        nullable=False,
    )
    auto_proc_program_id = Column(
        INTEGER(10),
        comment="Reference to the AutoProcProgram the cluster job is attached to",
        autoincrement=False,
    )
    start_time = Column(
        TIMESTAMP,
        comment="Start time of cluster job",
    )
    end_time = Column(
        TIMESTAMP,
        comment="End time of cluster job",
    )


class RelionJobInfo(Base):
    __tablename__ = "RelionJobInfo"

    job_id = Column(INTEGER(10), primary_key=True)
    cluster_id = Column(ForeignKey("ClusterJobInfo.cluster_id"))
    relion_start_time = Column(
        TIMESTAMP,
        comment="Start time of Relion job",
    )
    num_micrographs = Column(
        INTEGER(10),
        comment="Number of micrographs processed by the job if applicable",
        autoincrement=False,
    )
    job_name = Column(
        String(250),
        nullable=False,
        comment="Name of Relion job",
    )

    ClusterJobInfo = relationship("ClusterJobInfo")


def buffer_url() -> str:
    import ispyb.sqlalchemy

    sqlalchemy_url = ispyb.sqlalchemy.url()
    local_url = "/".join(sqlalchemy_url.split("/")[:-1]) + "/zocalo"
    return local_url
