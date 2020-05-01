from abc import abstractmethod, ABCMeta


class ParseDocumentError(RuntimeError):
    def __init__(self, provenance, message):
        super().__init__(message)
        self.provenance = provenance


class ParsedDocument(metaclass=ABCMeta):
    @abstractmethod
    def get_entities(self):
        pass
