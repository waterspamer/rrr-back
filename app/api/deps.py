from fastapi import Header, Query, Request

from app.core.errors import unauthorized
from app.services.vehicle_bundle_storage import VehicleBundleStorage
from app.services.vehicle_content_registry import VehicleContentRegistry
from app.services.vehicle_offer_registry import VehicleOfferRegistry
from app.services.runtime import RuntimeState


def get_runtime(request: Request) -> RuntimeState:
    return request.app.state.runtime


def get_vehicle_content_registry(request: Request) -> VehicleContentRegistry:
    return request.app.state.vehicle_content_registry


def get_vehicle_bundle_storage(request: Request) -> VehicleBundleStorage:
    return request.app.state.vehicle_bundle_storage


def get_vehicle_offer_registry(request: Request) -> VehicleOfferRegistry:
    return request.app.state.vehicle_offer_registry


def get_bearer_token(authorization: str | None = Header(default=None)) -> str:
    if not authorization or not authorization.startswith("Bearer "):
        raise unauthorized()
    token = authorization.split(" ", 1)[1].strip()
    if not token:
        raise unauthorized()
    return token


def get_admin_token(
    request: Request,
    authorization: str | None = Header(default=None),
    x_admin_token: str | None = Header(default=None),
    token: str | None = Query(default=None),
) -> str | None:
    if authorization and authorization.startswith("Bearer "):
        bearer = authorization.split(" ", 1)[1].strip()
        if bearer:
            return bearer
    if x_admin_token:
        return x_admin_token.strip()
    if token:
        return token.strip()
    query_token = request.query_params.get("token")
    if query_token:
        return query_token.strip()
    return None
