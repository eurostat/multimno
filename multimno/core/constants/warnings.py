class Warnings:
    size_data_variability = "The number of events in {type_of_data} data is unexpectedly low/high with respect to previous period, please check if there have been issues in the network"
    size_data_upper_lower = "The number of events in {type_of_data} data is over/under the threshold, please check if there have been changes in the network"

    error_rate_over_average = "Error rate for {variables} after syntactic checks application is unexpectedly high with respect to previous period"
    error_rate_upper_variability = "Error rate for {variables} after syntactic checks application is unexpectedly high with respect to previous period, taking into account its usual variability"
    error_rate_upper_limit = "Error rate for {variables} after syntactic checks application is over the threshold"

    error_type_rate_over_average = "{error_type_name}, {field_name} after syntactic checks application is unexpectedly high with respect to previous period"
    error_type_rate_upper_variability = "{error_type_name}, {field_name} after syntactic checks application is unexpectedly high with respect to previous period, taking into account its usual variability"
    error_type_rate_upper_limit = (
        "{error_type_name}, {field_name} after syntactic checks application is over the threshold"
    )
