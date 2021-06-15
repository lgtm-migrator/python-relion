import pytest
import relion
from relion.dbmodel.dbnode import DBNode
from relion.dbmodel import modeltables
from relion.dbmodel.modeltables import pid


@pytest.fixture
def proj(dials_data):
    return relion.Project(dials_data("relion_tutorial_data"))


@pytest.fixture
def mc_table(proj):
    table = modeltables.MotionCorrectionTable()
    mc_res = proj.motioncorrection["job002"]
    mc_db_entries = proj.motioncorrection.db_unpack(mc_res)
    for entry in mc_db_entries:
        table.add_row(entry)
    return table


@pytest.fixture
def ctf_table(proj):
    table = modeltables.CTFTable()
    ctf_res = proj.ctffind["job003"]
    ctf_db_entries = proj.ctffind.db_unpack(ctf_res)
    for i, entry in enumerate(ctf_db_entries):
        table.add_row({**entry, **{"motion_correction_id": i + 1}})
    return table


@pytest.fixture
def mc_db_node(mc_table):
    node = DBNode("MCTable", [modeltables.MotionCorrectionTable()])
    return node


@pytest.fixture
def ctf_db_node(ctf_table):
    node = DBNode("CTFTable", [modeltables.CTFTable()])
    return node


def test_correct_motion_correction_inserts_on_mc_table(mc_table):
    pid.reset(1)
    assert len(mc_table["motion_correction_id"]) == 24
    first_row = mc_table.get_row_by_primary_key(1)
    assert (
        first_row["micrograph_full_path"]
        == "MotionCorr/job002/Movies/20170629_00021_frameImage.mrc"
    )
    second_row = mc_table.get_row_by_primary_key(2)
    assert second_row["total_motion"] == "19.551677"


def test_correct_inserts_on_ctf_table(ctf_table):
    pid.reset(1)
    first_row = ctf_table.get_row_by_primary_key(1)
    assert len(ctf_table["ctf_id"]) == 24
    assert first_row["astigmatism"] == "288.135742"


def test_boolean_db_node(mc_db_node):
    assert mc_db_node


def test_inserting_to_mc_table_through_dbnode(mc_db_node, proj):
    pid.reset(1)
    mc_res = proj.motioncorrection["job002"]
    mc_db_entries = proj.motioncorrection.db_unpack(mc_res)
    mc_db_node.environment["end_time"] = 100
    mc_db_node.environment["extra_options"] = proj.run_options
    mc_db_node.environment.update(mc_db_entries)
    mc_db_node()
    first_row = mc_db_node.tables[0].get_row_by_primary_key(1)
    assert len(mc_db_node.tables[0]["motion_correction_id"]) == 24
    assert (
        first_row["micrograph_full_path"]
        == "MotionCorr/job002/Movies/20170629_00021_frameImage.mrc"
    )


def test_inserting_to_ctf_table_through_dbnode(mc_db_node, ctf_db_node, proj):
    pid.reset(1)
    mc_res = proj.motioncorrection["job002"]
    mc_db_entries = proj.motioncorrection.db_unpack(mc_res)
    mc_db_node.environment["end_time"] = 100
    mc_db_node.environment["extra_options"] = proj.run_options
    mc_db_node.environment.update(mc_db_entries)
    mc_db_node()
    ctf_res = proj.ctffind["job003"]
    ctf_db_entries = proj.ctffind.db_unpack(ctf_res)
    assert len(ctf_db_entries) == 24
    assert (
        mc_db_node.tables[0].get_row_index(
            "micrograph_full_path",
            "MotionCorr/job002/Movies/20170629_00021_frameImage.mrc",
        )
        == 0
    )
    ctf_db_node.environment["end_time"] = 100
    ctf_db_node.environment["extra_options"] = proj.run_options
    ctf_db_node.environment["check_for"] = "micrograph_full_path"
    ctf_db_node.environment["foreign_key"] = "motion_correction_id"
    ctf_db_node.environment["table_key"] = "motion_correction_id"
    ctf_db_node.environment["foreign_table"] = mc_db_node.tables[0]
    ctf_db_node.environment.update(ctf_db_entries)
    ctf_db_node()
    first_row = ctf_db_node.tables[0].get_row_by_primary_key(25)
    assert len(ctf_db_node.tables[0]["ctf_id"]) == 24
    assert first_row["motion_correction_id"] == 1
