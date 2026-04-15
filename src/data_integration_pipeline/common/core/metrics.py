from data_integration_pipeline.common.io.logger import logger
from pydantic import BaseModel, Field, computed_field


# TODO you'd want to have a connnection to prometheus or OTEL
class Metrics(BaseModel):
    success: int = Field(default=0)
    failures: int = Field(default=0)

    def log_result(self, is_success: bool, total: int = 1):
        if is_success:
            self.success += total
        else:
            self.failures += total

    @computed_field
    def total(self) -> int:
        return self.success + self.failures

    def join(self, metrics: 'Metrics'):
        self.success += metrics.success
        self.failures += metrics.failures

    def __str__(self):
        success_rate = (self.success / self.total * 100) if self.total > 0 else 0
        return '\n'.join(
            [f'\t✅ Success:\t{self.success}', f'\t❌ Errors:\t{self.failures}', f'\t🔢 Total:\t{self.total}', f'\t📈 Rate:\t{success_rate:.2f}%']
        )


def log_metrics(metrics: Metrics):
    logger.info(f'Processing metrics: {metrics.model_dump()}')


if __name__ == '__main__':
    metadata = Metrics(success=1, failures=2)
    print(metadata)
