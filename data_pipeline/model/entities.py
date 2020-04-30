
class BaseEntity:

    def __init__(self, data: dict):
        self.data = data


class Person(BaseEntity):
    pass


class PersonV2(BaseEntity):
    pass


class Manuscript(BaseEntity):
    pass


class ManuscriptVersion(BaseEntity):
    pass
