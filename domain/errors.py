"""Domain-level exceptions.

Pure business errors with no dependency on infrastructure. The original
``application``/``synchronization`` code imports these by the same names, so
the class names are preserved for compatibility.
"""

from __future__ import annotations


class DomainError(Exception):
    """Base domain exception."""

    pass


class DuplicateMappingError(DomainError):
    """Raised when a seller SKU already has an active mapping."""

    def __init__(self, seller_sku: str):
        self.seller_sku = seller_sku
        super().__init__(f"Seller SKU '{seller_sku}' already has an active mapping.")


class EntityNotFoundError(DomainError):
    """Raised when an entity is not found."""

    def __init__(self, entity_name: str, entity_id):
        self.entity_name = entity_name
        self.entity_id = entity_id
        super().__init__(f"{entity_name} with id '{entity_id}' not found.")


class InvenTreePartNotFoundError(DomainError):
    """Raised when a part is not found in InvenTree."""

    def __init__(self, seller_sku: str):
        self.seller_sku = seller_sku
        super().__init__(f"InvenTree part not found for seller SKU '{seller_sku}'.")


class ConfigurationError(ValueError):
    """Raised when an adapter is misconfigured for a call.

    Subclass of ``ValueError`` so callers using broad ``except ValueError``
    keep working. Introduced as the generic replacement for the
    APISIX-specific config error; the infrastructure layer keeps a
    ``ApisixConfigError`` alias for backward compatibility.
    """

    pass
