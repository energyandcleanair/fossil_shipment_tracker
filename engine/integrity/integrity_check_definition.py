from base.logger import logger


class IntegrityCheckResult:
    def __init__(self, step, error=None):
        self.step = step
        self.error = error

    @property
    def success(self):
        return self.error == None

    @property
    def name(self):
        return self.step.name

    def format_error(self):
        return f"Integrity check {self.name} failed with error: {self.error}"


class IntegrityCheckDefinition:
    def __init__(self, name, test):
        self.name = name
        self.test = test

    def run_test(self):
        logger.info(f"Checking integrity: {self.name}")
        try:
            self.test()
            return IntegrityCheckResult(self)
        except AssertionError as e:
            logger.info(f"Checking integrity {self.name} - failure: {e}")
            message = str(e)
            return IntegrityCheckResult(self, error=message)
