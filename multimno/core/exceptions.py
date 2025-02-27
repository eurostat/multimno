class ComponentNotSupported(Exception):
    def __init__(self, component_id) -> None:
        super().__init__(f"Component {component_id} is not supported.")


class PpNoDevicesException(Exception):
    """
    Exception raised when no devices are found at the timepoint being calculated by the PresentPopulation class.
    """

    def error_msg(self, time_point: str):
        return f"No devices found at {time_point} timepoint. Please check the MNO data of this day."


class CriticalQualityWarningRaisedException(Exception):
    """
    Exception raised when a component raises quality warnings.
    """

    def __init__(self, component_id: str) -> None:
        self.component_id = component_id
        self.error_msg = (
            f"Critical Quality Warnings were raised during the execution of the component: {self.component_id}."
        )
        super().__init__(self.error_msg)
