from dataclasses import dataclass

@dataclass
class SensorRecord:
    IdSensore: int
    Data: str
    Valore: float
    Stato: str
    idOperatore: int

    def __post_init__(self):
        self.IdSensore = int(self.IdSensore)
        self.Valore = float(self.Valore)
        self.idOperatore = int(self.idOperatore)
