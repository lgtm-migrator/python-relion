import sqlalchemy

from relion.zocalo import alchemy


def run():
    url = alchemy.buffer_url()
    engine = sqlalchemy.create_engine(url, echo=True, connect_args={"use_pure": True})
    alchemy.ClusterMonitoring.__table__.create(engine)
