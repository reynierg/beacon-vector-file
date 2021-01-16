"""Contains logic to verify if a dict conforms with the expected key-value pairs

Will provide access to a dict key-value pairs using attributes of a class

This file can be imported as a module and contains the following classes:
    * JSONDocumentModel - provides validation and access to a dict key-value pairs
"""

from pydantic import BaseModel
from pydantic import Field


class JSONDocumentModel(BaseModel):
    """Models the structure of a JSON document in the input JSON file.

    It also implicitly enforce validation
    The expected dict format is the following:
    {
      "BeaconId": 113,
      "ant_id": 202,
      "dbm_ant": -58.97817436922068,
      "timestamp": "2016-11-22T09:48:00.00Z"
    }
    """

    beacon_id: int = Field(alias="BeaconId")
    ant_id: int
    dbm_ant: float
    timestamp: str

    class Config:
        """Prohibit the mutation of the model attributes"""

        allow_mutation = False
