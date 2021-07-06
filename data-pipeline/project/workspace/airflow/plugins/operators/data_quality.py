from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 dq_checks=[],
                 *args, **kwargs):
        """
        Initiate operator object
        :param dq_checks: the test function to run and the expected result
        :param args
        :param kwargs
        """
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.dq_checks = dq_checks

    def execute(self, context):
        """
        execute data validation against Redshift cluster
        :param context: operator context
        """
        for item in self.dq_checks:
            self.log.info(item)
            result = item['func'](*item['func_args'])
            if result != item['expected_result']:
                raise ValueError(
                    f"expected:{item['expected_result']} but received: {result}  for function {item['func']} "
                    f"with arguments {item['func_args']}")
        self.log.info("Done validating")
