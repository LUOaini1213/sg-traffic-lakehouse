import pytest

from lakehouse import repro


@pytest.mark.slow
def test_pipeline_twice_gives_identical_gold(settings, tmp_path):
    res = repro.run(settings, None, str(tmp_path / "repro"))
    assert res["identical_content"] and res["identical_files"]
    assert set(res["tables"]) == {"gold_link_speed_hourly", "gold_congestion_episodes", "gold_stop_demand_hourly",
                                  "gold_top_od_flows", "gold_link_hour_features"}
