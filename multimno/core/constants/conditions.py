class Conditions:
    size_data_variability = "Size is out of control limits calculated on the basis of average and standard deviation of the distribution of the size in previous period. Control limits =  (average ± X·{SD})"
    size_data_upper_lower = "The size is under/over thresholds {X} and {Y}"

    error_rate_over_average = "Error rate for {variables} is over previous period average by {X}%"
    error_rate_upper_variability = "Error rate for {variables} is over the upper control limit calculated on the basis of average and standard deviation of the distribution of the error rate in previous period. Upper Control limit = (average + X·{SD})"
    error_rate_upper_limit = "Error rate for {variables} is over the value {X}."

    error_type_rate_over_average = "{error_type_name}, {field_name} is over previous period average by {X}%"
    error_type_rate_upper_variability = "{error_type_name}, {field_name} is over the upper control limit calculated on the basis of average and standard deviation of the distribution of the error rate in previous period. Upper Control limit = (average + X·{SD})"
    error_type_rate_upper_limit = "{error_type_name}, {field_name} is over the value {X}."
