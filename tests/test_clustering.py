from earthquake_platform.clustering import haversine_km, candidate_clusters


def test_haversine_same_point_is_zero():
    assert haversine_km(0, 0, 0, 0) == 0


def test_candidate_cluster_groups_nearby_events():
    events = [
        {"id": "a", "time": 1_700_000_000_000, "longitude": 70.0, "latitude": 20.0, "magnitude": 5.0},
        {"id": "b", "time": 1_700_000_100_000, "longitude": 70.1, "latitude": 20.0, "magnitude": 3.0},
    ]
    clusters = candidate_clusters(events, max_days=7, max_distance_km=100, minimum_events=2)
    assert len(clusters) == 1
    assert clusters[0]["mainshock_id"] == "a"
    assert clusters[0]["potential_aftershock_count"] == 1


def test_far_events_not_grouped():
    events = [
        {"id": "a", "time": 1_700_000_000_000, "longitude": 0, "latitude": 0, "magnitude": 5.0},
        {"id": "b", "time": 1_700_000_100_000, "longitude": 100, "latitude": 40, "magnitude": 3.0},
    ]
    assert candidate_clusters(events, max_days=7, max_distance_km=100, minimum_events=2) == []
