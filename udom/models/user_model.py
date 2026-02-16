from udom.models.umodel import UModel

class User(UModel):
    id: int
    name: str
    age: int
    active: bool
