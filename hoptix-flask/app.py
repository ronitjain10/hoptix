import os
import uuid
import logging
from datetime import datetime
from flask import Flask, jsonify, request
from flask_cors import CORS
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Import our modules
from config import Settings
from integrations.db_supabase import Supa
from scripts.grade_from_csv import grade_from_csv
# Analytics service is now handled through routes/analytics.py
from routes.analytics import analytics_bp

# Configure logging for production - console and file
import os
log_handlers = [logging.StreamHandler()]

# Add file logging if LOG_FILE environment variable is set
if os.getenv('LOG_FILE'):
    log_handlers.append(logging.FileHandler(os.getenv('LOG_FILE')))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
    handlers=log_handlers
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Enable CORS for all routes
CORS(app, origins=["http://localhost:3000", "http://127.0.0.1:3000"])

# Register blueprints
app.register_blueprint(analytics_bp)

# Initialize database connection
try:
    settings = Settings()
    db = Supa(settings.SUPABASE_URL, settings.SUPABASE_SERVICE_KEY)
    app.config["DB"] = db
    logger.info("Successfully initialized database connection")
except Exception as e:
    logger.error(f"Failed to initialize database connection: {e}")
    raise

@app.get("/health")
def health():
    """Health check endpoint"""
    return jsonify({
        "ok": True, 
        "timestamp": datetime.now().isoformat(),
        "service": "hoptix-onboarding"
    })

@app.post("/onboard-restaurant")
def onboard_restaurant():
    """Onboard a new restaurant onto the Hoptix platform"""
    try:
        data = request.get_json()
        if not data:
            return jsonify({
                "success": False,
                "error": "JSON data required"
            }), 400
        
        # Validate required fields
        required_fields = ["restaurant_name", "location_name", "timezone"]
        missing_fields = [field for field in required_fields if not data.get(field)]
        if missing_fields:
            return jsonify({
                "success": False,
                "error": f"Missing required fields: {', '.join(missing_fields)}"
            }), 400
        
        restaurant_name = data["restaurant_name"]
        location_name = data["location_name"]
        timezone = data["timezone"]
        
        logger.info(f"Onboarding restaurant: {restaurant_name} - {location_name}")
        
        # Generate IDs
        org_id = str(uuid.uuid4())
        location_id = str(uuid.uuid4())
        
        # Create organization
        org_data = {
            "id": org_id,
            "name": restaurant_name,
            "created_at": datetime.now().isoformat()
        }
        db.client.table("orgs").insert(org_data).execute()
        logger.info(f"Created organization: {org_id}")
        
        # Create location
        location_data = {
            "id": location_id,
            "org_id": org_id,
            "name": location_name,
            "tz": timezone,
            "created_at": datetime.now().isoformat()
        }
        db.client.table("locations").insert(location_data).execute()
        logger.info(f"Created location: {location_id}")
        
        result = {
            "success": True,
            "message": f"Successfully onboarded {restaurant_name} - {location_name}",
            "data": {
                "org_id": org_id,
                "location_id": location_id,
                "restaurant_name": restaurant_name,
                "location_name": location_name,
                "timezone": timezone
            }
        }
        
        logger.info(f"Successfully onboarded restaurant: {org_id} / {location_id}")
        return jsonify(result)
        
    except Exception as e:
        logger.error(f"Error onboarding restaurant: {str(e)}", exc_info=True)
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.route("/grade-from-csv", methods=["POST"])
def grade_from_csv_endpoint():
    """Grade transactions directly from CSV export"""
    try:
        data = request.json or {}
        csv_path = data.get("csv_path")
        run_id_filter = data.get("run_id")
        
        if not csv_path:
            return jsonify({
                "success": False,
                "error": "csv_path is required"
            }), 400
        
        if not os.path.exists(csv_path):
            return jsonify({
                "success": False,
                "error": f"CSV file not found: {csv_path}"
            }), 404
        
        logger.info(f"Starting CSV grading for: {csv_path}")
        
        # Run the grading process
        grade_from_csv(csv_path, run_id_filter)
        
        return jsonify({
            "success": True,
            "message": f"Successfully processed CSV: {csv_path}",
            "csv_path": csv_path,
            "run_id_filter": run_id_filter
        })
        
    except Exception as e:
        logger.error(f"Error grading from CSV: {str(e)}", exc_info=True)
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.route("/analytics/comprehensive", methods=["GET"])
def get_comprehensive_analytics():
    """Get comprehensive analytics report for upsells, upsizes, and add-ons"""
    try:
        # Get query parameters
        run_id = request.args.get("run_id")
        date_from = request.args.get("date_from")
        date_to = request.args.get("date_to")
        item_filter = request.args.get("item_filter")
        
        logger.info(f"Fetching analytics data with filters: run_id={run_id}, date_from={date_from}, date_to={date_to}, item_filter={item_filter}")
        
        # Build query
        query = db.client.table("grades").select("*")
        
        # Apply filters
        if run_id:
            query = query.eq("run_id", run_id)
        if date_from:
            query = query.gte("created_at", date_from)
        if date_to:
            query = query.lte("created_at", date_to)
        
        # Execute query
        result = query.execute()
        
        if not result.data:
            return jsonify({
                "success": True,
                "message": "No data found for the specified filters",
                "data": {
                    "summary": {"total_transactions": 0},
                    "upselling": {"total_opportunities": 0},
                    "upsizing": {"total_opportunities": 0},
                    "addons": {"total_opportunities": 0}
                }
            })
        
        # Convert to format expected by analytics service
        transactions = []
        for row in result.data:
            # Map database columns to CSV format expected by analytics service
            transaction = {
                "Transaction ID": row.get("transaction_id", ""),
                "Date": row.get("created_at", "").split("T")[0] if row.get("created_at") else "",
                "Complete Transcript?": row.get("complete_order", 0),
                "Items Initially Requested": row.get("items_initial", "0"),
                "# of Items Ordered": row.get("num_items_initial", 0),
                "# of Chances to Upsell": row.get("num_upsell_opportunities", 0),
                "Items that Could be Upsold": row.get("items_upsellable", "0"),
                "# of Upselling Offers Made": row.get("num_upsell_offers", 0),
                "Items Succesfully Upsold": row.get("items_upsold", "0"),
                "# of Sucessfull Upselling chances": row.get("num_upsell_success", 0),
                "# of Times largest Option Offered": row.get("num_largest_offers", 0),
                "# of Chances to Upsize": row.get("num_upsize_opportunities", 0),
                "Items in Order that could be Upsized": row.get("items_upsizeable", "0"),
                "# of Upsizing Offers Made": row.get("num_upsize_offers", 0),
                "# of Sucessfull Upsizing chances": row.get("num_upsize_success", 0),
                "Items Successfully Upsized": row.get("items_upsize_success", "0"),
                "# of Chances to Add-on": row.get("num_addon_opportunities", 0),
                "Items in Order that could have Add-Ons": row.get("items_addonable", "0"),
                "# of Add-on Offers": row.get("num_addon_offers", 0),
                "# of Succesful Add-on Offers": row.get("num_addon_success", 0),
                "Items with Successful Add-Ons": row.get("items_addon_success", "0"),
                "Items Ordered After Upsizing, Upselling, and Add-on Offers": row.get("items_after", "0"),
                "# of Items Ordered After Upselling, Upsizing, and Add-on Offers": row.get("num_items_after", 0),
            }
            transactions.append(transaction)
        
        logger.info(f"Processing {len(transactions)} transactions for analytics")
        
        # Generate analytics report
        analytics_service = HoptixAnalyticsService()
        
        if item_filter:
            report = analytics_service.get_item_specific_report(transactions, item_filter)
        else:
            report = analytics_service.generate_comprehensive_report(transactions)
        
        return jsonify({
            "success": True,
            "data": report,
            "filters_applied": {
                "run_id": run_id,
                "date_from": date_from,
                "date_to": date_to,
                "item_filter": item_filter
            }
        })
        
    except Exception as e:
        logger.error(f"Error generating analytics report: {str(e)}", exc_info=True)
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.route("/analytics/summary", methods=["GET"])
def get_analytics_summary():
    """Get a quick summary of analytics metrics"""
    try:
        # Get query parameters
        run_id = request.args.get("run_id")
        
        logger.info(f"Fetching analytics summary for run_id={run_id}")
        
        # Build query
        query = db.client.table("grades").select("*")
        if run_id:
            query = query.eq("run_id", run_id)
        
        # Execute query
        result = query.execute()
        
        if not result.data:
            return jsonify({
                "success": True,
                "data": {
                    "total_transactions": 0,
                    "total_upsell_opportunities": 0,
                    "total_upsell_successes": 0,
                    "total_upsize_opportunities": 0,
                    "total_upsize_successes": 0,
                    "total_addon_opportunities": 0,
                    "total_addon_successes": 0,
                    "overall_success_rate": 0
                }
            })
        
        # Calculate summary metrics
        total_transactions = len(result.data)
        total_upsell_opportunities = sum(row.get("num_upsell_opportunities", 0) for row in result.data)
        total_upsell_successes = sum(row.get("num_upsell_success", 0) for row in result.data)
        total_upsize_opportunities = sum(row.get("num_upsize_opportunities", 0) for row in result.data)
        total_upsize_successes = sum(row.get("num_upsize_success", 0) for row in result.data)
        total_addon_opportunities = sum(row.get("num_addon_opportunities", 0) for row in result.data)
        total_addon_successes = sum(row.get("num_addon_success", 0) for row in result.data)
        
        total_opportunities = total_upsell_opportunities + total_upsize_opportunities + total_addon_opportunities
        total_successes = total_upsell_successes + total_upsize_successes + total_addon_successes
        
        overall_success_rate = (total_successes / total_opportunities * 100) if total_opportunities > 0 else 0
        
        return jsonify({
            "success": True,
            "data": {
                "total_transactions": total_transactions,
                "total_upsell_opportunities": total_upsell_opportunities,
                "total_upsell_successes": total_upsell_successes,
                "upsell_success_rate": (total_upsell_successes / total_upsell_opportunities * 100) if total_upsell_opportunities > 0 else 0,
                "total_upsize_opportunities": total_upsize_opportunities,
                "total_upsize_successes": total_upsize_successes,
                "upsize_success_rate": (total_upsize_successes / total_upsize_opportunities * 100) if total_upsize_opportunities > 0 else 0,
                "total_addon_opportunities": total_addon_opportunities,
                "total_addon_successes": total_addon_successes,
                "addon_success_rate": (total_addon_successes / total_addon_opportunities * 100) if total_addon_opportunities > 0 else 0,
                "total_opportunities": total_opportunities,
                "total_successes": total_successes,
                "overall_success_rate": overall_success_rate
            }
        })
        
    except Exception as e:
        logger.error(f"Error generating analytics summary: {str(e)}", exc_info=True)
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.route("/analytics/items", methods=["GET"])
def get_item_analytics():
    """Get analytics broken down by individual items"""
    try:
        # Get query parameters
        run_id = request.args.get("run_id")
        limit = int(request.args.get("limit", 20))
        
        logger.info(f"Fetching item analytics for run_id={run_id}, limit={limit}")
        
        # Build query
        query = db.client.table("grades").select("*")
        if run_id:
            query = query.eq("run_id", run_id)
        
        # Execute query
        result = query.execute()
        
        if not result.data:
            return jsonify({
                "success": True,
                "data": {"items": {}}
            })
        
        # Convert to format expected by analytics service
        transactions = []
        for row in result.data:
            transaction = {
                "Items Initially Requested": row.get("items_initial", "0"),
                "# of Chances to Upsell": row.get("num_upsell_opportunities", 0),
                "# of Upselling Offers Made": row.get("num_upsell_offers", 0),
                "# of Sucessfull Upselling chances": row.get("num_upsell_success", 0),
                "# of Chances to Upsize": row.get("num_upsize_opportunities", 0),
                "# of Upsizing Offers Made": row.get("num_upsize_offers", 0),
                "# of Sucessfull Upsizing chances": row.get("num_upsize_success", 0),
                "# of Chances to Add-on": row.get("num_addon_opportunities", 0),
                "# of Add-on Offers": row.get("num_addon_offers", 0),
                "# of Succesful Add-on Offers": row.get("num_addon_success", 0),
            }
            transactions.append(transaction)
        
        # Generate analytics
        analytics_service = HoptixAnalyticsService()
        upsell_metrics = analytics_service.upsell_analytics.calculate_upsell_metrics(transactions)
        upsize_metrics = analytics_service.upsize_analytics.calculate_upsize_metrics(transactions)
        addon_metrics = analytics_service.addon_analytics.calculate_addon_metrics(transactions)
        
        # Combine item metrics
        all_items = set(
            list(upsell_metrics["by_item"].keys()) +
            list(upsize_metrics["by_item"].keys()) +
            list(addon_metrics["by_item"].keys())
        )
        
        item_analytics = {}
        for item in all_items:
            item_analytics[item] = {
                "upselling": upsell_metrics["by_item"].get(item, {"opportunities": 0, "offers": 0, "successes": 0, "success_rate": 0, "offer_rate": 0}),
                "upsizing": upsize_metrics["by_item"].get(item, {"opportunities": 0, "offers": 0, "successes": 0, "success_rate": 0, "offer_rate": 0}),
                "addons": addon_metrics["by_item"].get(item, {"opportunities": 0, "offers": 0, "successes": 0, "success_rate": 0, "offer_rate": 0})
            }
        
        # Sort by total opportunities and limit
        sorted_items = sorted(
            item_analytics.items(),
            key=lambda x: (
                x[1]["upselling"]["opportunities"] +
                x[1]["upsizing"]["opportunities"] +
                x[1]["addons"]["opportunities"]
            ),
            reverse=True
        )[:limit]
        
        return jsonify({
            "success": True,
            "data": {
                "items": dict(sorted_items),
                "total_items_analyzed": len(all_items),
                "showing_top": min(limit, len(all_items))
            }
        })
        
    except Exception as e:
        logger.error(f"Error generating item analytics: {str(e)}", exc_info=True)
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=False)