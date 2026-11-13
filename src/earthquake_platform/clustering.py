
"""Explainable spatiotemporal candidate earthquake-cluster detector."""

from math import radians, sin, cos, asin, sqrt


def haversine_km(lon1, lat1, lon2, lat2):
    """Calculate great-circle distance in kilometres."""
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    dlon = lon2 - lon1
    dlat = lat2 - lat1

    a = (
        sin(dlat / 2) ** 2
        + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    )

    return 6371.0088 * 2 * asin(sqrt(a))


def candidate_clusters(
    events,
    max_days=7,
    max_distance_km=100,
    minimum_events=2
):
    """
    Group earthquakes around a mainshock.

    Every member must be within the configured time and
    distance limits of the selected mainshock.

    This is a candidate detector, not an official
    seismological aftershock classifier.
    """

    valid = [
        e for e in events
        if e.get("id")
        and e.get("time") is not None
        and e.get("longitude") is not None
        and e.get("latitude") is not None
        and e.get("magnitude") is not None
    ]

    # Process larger earthquakes first.
    ordered = sorted(
        valid,
        key=lambda e: float(e["magnitude"]),
        reverse=True
    )

    max_ms = max_days * 24 * 60 * 60 * 1000
    assigned = set()
    output = []

    for mainshock in ordered:

        if mainshock["id"] in assigned:
            continue

        main_time = int(mainshock["time"])
        main_lon = float(mainshock["longitude"])
        main_lat = float(mainshock["latitude"])
        main_mag = float(mainshock["magnitude"])

        members = [mainshock]

        for event in ordered:

            if event["id"] in assigned:
                continue

            if event["id"] == mainshock["id"]:
                continue

            event_time = int(event["time"])

            # Only consider events after the mainshock.
            if event_time <= main_time:
                continue

            # Apply the time window.
            if event_time - main_time > max_ms:
                continue

            distance = haversine_km(
                main_lon,
                main_lat,
                float(event["longitude"]),
                float(event["latitude"])
            )

            # Apply the distance and magnitude conditions.
            if (
                distance <= max_distance_km
                and float(event["magnitude"]) < main_mag
            ):
                members.append(event)

        if len(members) < minimum_events:
            continue

        for event in members:
            assigned.add(event["id"])

        lons = [float(e["longitude"]) for e in members]
        lats = [float(e["latitude"]) for e in members]
        times = [int(e["time"]) for e in members]

        output.append({
            "mainshock_id": mainshock["id"],
            "mainshock_magnitude": main_mag,
            "event_count": len(members),
            "potential_aftershock_count": len(members) - 1,
            "min_longitude": min(lons),
            "max_longitude": max(lons),
            "min_latitude": min(lats),
            "max_latitude": max(lats),
            "start_time_ms": min(times),
            "end_time_ms": max(times),
            "events": members,
        })

    return output
