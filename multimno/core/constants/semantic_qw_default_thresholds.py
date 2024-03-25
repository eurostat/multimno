"""
Contains the default threshold values used by the Event Device Semantic Quality Warnings
"""

SEMANTIC_DEFAULT_THRESHOLDS = {
    "CELL_ID_NON_EXISTENT": {"sd_lookback_days": 7, "min_sd": 2, "min_percentage": 0},
    "CELL_ID_NOT_VALID": {"sd_lookback_days": 7, "min_sd": 2, "min_percentage": 0},
    "INCORRECT_EVENT_LOCATION": {"sd_lookback_days": 7, "min_sd": 2, "min_percentage": 0},
    "SUSPICIOUS_EVENT_LOCATION": {"sd_lookback_days": 7, "min_sd": 2, "min_percentage": 0},
}
