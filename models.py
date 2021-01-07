import pydantic


class JSONDocumentModel(pydantic.BaseModel):
    """
    Models the structure of a JSON document in the input JSON file.
    It also implicitly enforce validation
    """
    # {"BeaconId":113,"ant_id":202,"dbm_ant":-58.97817436922068,"timestamp":"2016-11-22T09:48:00.00Z"},
    beacon_id: int = pydantic.Field(alias='BeaconId')
    ant_id: int
    dbm_ant: float
    timestamp: str

    class Config:
        allow_mutation = False
