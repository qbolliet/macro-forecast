# Importation des modules
import json
from pathlib import Path
from typing import Any
import pandas as pd


# Fonction de chargement des données depuis un fichier xls en local
def load_local(filepath: str, **kwargs) -> pd.DataFrame:
    """Load a JSON file from local storage.

    Args:
        filepath (str): Path to the local xls file.
        **kwargs: Additional arguments forwarded to ``pd.read_excel``.

    Returns:
        Any: The DataFrame containing the data of the xls file.

    Raises:
        ValueError: If the file extension is not ``.xls``.
        FileNotFoundError: If the file does not exist.

    Examples:
        >>> data = load_local('config.json')
        >>> data = load_local('records.json')
    """
    # Vérification de l'extension
    extension = Path(filepath).suffix.lower()[1:]
    if extension != "xls":
        raise ValueError(
            f"Unsupported extension '.{extension}': only '.xls' files are supported."
        )
    # Lecture du fichier JSON
    with open(filepath, "r", encoding="utf-8") as f:
        return pd.read_excel(f, engine="xlrd", **kwargs)
