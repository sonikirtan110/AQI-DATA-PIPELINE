from unittest.mock import MagicMock, patch


def test_fetch_all_records_returns_list():
    mock_response = MagicMock()
    mock_response.json.return_value = {
        "records": [
            {
                "station": "S01",
                "city": "Delhi",
                "pollutant_id": "PM2.5",
                "pollutant_avg": 85.0,
                "last_update": "2026-03-19 06:00:00",
            }
        ],
        "total": "1",
    }
    mock_response.raise_for_status.return_value = None

    with patch("include.scripts.fetch_ogd.API_KEY", "dummy-key"):
        with patch("include.scripts.fetch_ogd.requests.get", return_value=mock_response):
            from include.scripts.fetch_ogd import fetch_all_records

            records = fetch_all_records()

    assert isinstance(records, list)
    assert len(records) == 1
    assert records[0]["city"] == "Delhi"


def test_pm25_category_threshold_reference():
    value = 160.0
    category = (
        "Good"
        if value <= 12
        else "Moderate"
        if value <= 35.4
        else "Unhealthy for Sensitive Groups"
        if value <= 55.4
        else "Unhealthy"
        if value <= 150.4
        else "Very Unhealthy"
        if value <= 250.4
        else "Hazardous"
    )
    assert category == "Very Unhealthy"
