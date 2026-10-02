"""
DEPRECATED: Replaced by VariantService in variant_service.py.
"""
import warnings
warnings.warn(
    "sku_service is deprecated, use variant_service instead",
    DeprecationWarning,
    stacklevel=2,
)

from .variant_service import VariantService as SkuService
