from data_pipeline.utils.xml_transform_util.timestamp import parse_timestamp


class BaseEntity:

    def __init__(self, data: dict):
        self.data = data


class Person(BaseEntity):

    def __init__(self, data: dict):
        super().__init__(data)
        self.person_id = data['person_id']
        self.modified_timestamp = parse_timestamp(data['modified_timestamp'])


class PersonV2(BaseEntity):

    def __init__(self, data: dict):
        super().__init__(data)
        self.person_id = data['person_id']
        self.modified_timestamp = parse_timestamp(data['modified_timestamp'])


class Manuscript(BaseEntity):

    def __init__(self, data: dict):
        super().__init__(data)
        self.manuscript_id = data['manuscript_id']
        self.modified_timestamp = parse_timestamp(data['modified_timestamp'])


class ManuscriptVersion(BaseEntity):

    def __init__(self, data: dict):
        super().__init__(data)
        self.manuscript_id = data['manuscript_id']
        self.created_timestamp = parse_timestamp(data['created_timestamp'])
        self.modified_timestamp = parse_timestamp(data['modified_timestamp'])
