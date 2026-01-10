import json
import azure.functions as func

from integral_core import compute_integrals

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)


@app.route(route="numericalintegralservice/{lower}/{upper}", methods=["GET"])
def numericalintegralservice(req: func.HttpRequest) -> func.HttpResponse:
    lower = req.route_params.get("lower")
    upper = req.route_params.get("upper")

    # validation (same as your Flask version)
    try:
        lo = float(lower)
        up = float(upper)
    except (TypeError, ValueError):
        return func.HttpResponse(
            json.dumps({"error": "lower and upper must be numbers"}),
            status_code=400,
            mimetype="application/json",
        )

    if up <= lo:
        return func.HttpResponse(
            json.dumps({"error": "upper must be greater than lower"}),
            status_code=400,
            mimetype="application/json",
        )

    payload = compute_integrals(lo, up)
    return func.HttpResponse(
        json.dumps(payload),
        status_code=200,
        mimetype="application/json",
    )

