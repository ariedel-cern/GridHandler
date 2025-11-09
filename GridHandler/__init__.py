# GridHandler/__init__.py

# check if ROOT is present
try:
    import ROOT
except ImportError as e:
    raise ImportError(
        "GridHandler requires PyROOT (ROOT's Python bindings). "
        "Please install ROOT (e.g. via aliBuild, alisw, or from https://root.cern)."
    ) from e

try:
    import alienpy
except ImportError as e:
    raise ImportError(
        "GridHandler requires alienpy to connect to the grid. "
        "Please install xjalienfs (e.g. via aliBuild). "
    ) from e


__all__ = ["GridHandler"]
