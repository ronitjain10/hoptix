#!/usr/bin/env python3
"""
Analytics API Routes for Hoptix

Provides REST API endpoints for accessing analytics data.
"""

from flask import Blueprint, request, jsonify
from integrations.db_supabase import Supa
# Use the working analytics logic inline for now
from config import Settings
import logging

logger = logging.getLogger(__name__)

# Create blueprint
analytics_bp = Blueprint('analytics', __name__, url_prefix='/api/analytics')

# Initialize services
settings = Settings()
db = Supa(settings.SUPABASE_URL, settings.SUPABASE_SERVICE_KEY)

def load_item_prices():
    """Load item prices from all pricing files: meals.json, items.json, add_ons.json, misc_items.json"""
    try:
        import json
        import os
        
        prompts_dir = os.path.join(os.path.dirname(__file__), '..', 'prompts')
        item_prices = {}
        
        # List of files to check for pricing
        pricing_files = ['meals.json', 'items.json', 'add_ons.json', 'misc_items.json']
        
        for filename in pricing_files:
            file_path = os.path.join(prompts_dir, filename)
            if os.path.exists(file_path):
                try:
                    with open(file_path, 'r') as f:
                        data = json.load(f)
                    
                    # Handle different data structures
                    items_data = data if isinstance(data, list) else data.get('items', [])
                    
                    for item in items_data:
                        item_id = item.get('Item ID')
                        if item_id is None:
                            continue
                            
                        # Handle different price structures
                        if 'Price' in item:
                            # Single price
                            price = item.get('Price', 0)
                            size_ids = item.get('Size IDs', [0])
                            
                            # Create entries for each size variation
                            for size_id in size_ids:
                                key = f"{item_id}_{size_id}"
                                item_prices[key] = price
                                
                        elif 'Prices' in item:
                            # Multiple prices by size
                            prices_dict = item.get('Prices', {})
                            for size_id, price in prices_dict.items():
                                key = f"{item_id}_{size_id}"
                                item_prices[key] = price
                    
                    logger.info(f"Loaded {len([i for i in items_data if i.get('Item ID')])} items from {filename}")
                        
                except Exception as e:
                    logger.error(f"Error loading {filename}: {e}")
                    continue
            else:
                logger.warning(f"Pricing file not found: {filename}")
        
        logger.info(f"Loaded total of {len(item_prices)} item price entries from all files")
        return item_prices
    except Exception as e:
        logger.error(f"Error loading item prices: {e}")
        return {}

def get_item_price(item_id: str, item_prices: dict) -> float:
    """Get price for an item, with fallback logic"""
    if not item_id or not item_prices:
        return 0.0
    
    # Try exact match first
    if item_id in item_prices:
        return item_prices[item_id]
    
    # Try parsing item_id to get base ID and size
    try:
        if '_' in item_id:
            base_id, size_id = item_id.split('_', 1)
            # Try with size 0 as fallback
            fallback_key = f"{base_id}_0"
            if fallback_key in item_prices:
                return item_prices[fallback_key]
    except:
        pass
    
    # Default fallback price
    return 5.0

def generate_analytics_report(run_id: str, data: list) -> dict:
    """Generate analytics report using the working logic"""
    from collections import defaultdict, Counter
    from datetime import datetime
    
    # Load item prices
    item_prices = load_item_prices()
    
    # Calculate summary metrics
    total_transactions = len(data)
    complete_transactions = sum(1 for t in data if t.get("Complete Transcript?", 0) == 1)
    
    # Calculate average items
    total_items_initial = sum(t.get("# of Items Ordered", 0) for t in data)
    total_items_final = sum(t.get("# of Items Ordered After Upselling, Upsizing, and Add-on Offers", 0) for t in data)
    
    summary = {
        "total_transactions": total_transactions,
        "complete_transactions": complete_transactions,
        "completion_rate": (complete_transactions / total_transactions * 100) if total_transactions > 0 else 0,
        "avg_items_initial": total_items_initial / total_transactions if total_transactions > 0 else 0,
        "avg_items_final": total_items_final / total_transactions if total_transactions > 0 else 0,
        "avg_item_increase": (total_items_final - total_items_initial) / total_transactions if total_transactions > 0 else 0
    }
    
    # Calculate upselling metrics
    upsell_opportunities = sum(t.get("# of Chances to Upsell", 0) for t in data)
    upsell_offers = sum(t.get("# of Upselling Offers Made", 0) for t in data)
    upsell_successes = sum(t.get("# of Sucessfull Upselling chances", 0) for t in data)
    
    # Calculate upselling revenue using actual item prices
    upsell_revenue = 0.0
    for transaction in data:
        upsold_items_field = transaction.get("Items Succesfully Upsold", "0")
        if upsold_items_field and upsold_items_field != "0":
            upsold_items = []
            if isinstance(upsold_items_field, str):
                try:
                    import json
                    parsed = json.loads(upsold_items_field)
                    if isinstance(parsed, list):
                        upsold_items = [str(item) for item in parsed]
                    elif isinstance(parsed, dict):
                        upsold_items = list(parsed.keys())
                    else:
                        upsold_items = [str(parsed)]
                except:
                    if upsold_items_field != "0":
                        upsold_items = [upsold_items_field]
            elif isinstance(upsold_items_field, (list, dict)):
                upsold_items = list(upsold_items_field) if isinstance(upsold_items_field, list) else list(upsold_items_field.keys())
            
            for item in upsold_items:
                upsell_revenue += get_item_price(item, item_prices)
    
    upselling = {
        "total_opportunities": upsell_opportunities,
        "total_offers": upsell_offers,
        "total_successes": upsell_successes,
        "total_revenue": upsell_revenue,
        "avg_revenue_per_success": upsell_revenue / upsell_successes if upsell_successes > 0 else 0,
        "success_rate": (upsell_successes / upsell_offers * 100) if upsell_offers > 0 else 0,
        "offer_rate": (upsell_offers / upsell_opportunities * 100) if upsell_opportunities > 0 else 0,
        "conversion_rate": (upsell_successes / upsell_opportunities * 100) if upsell_opportunities > 0 else 0,
    }
    
    # Calculate upsizing metrics
    upsize_opportunities = sum(t.get("# of Chances to Upsize", 0) for t in data)
    upsize_offers = sum(t.get("# of Upsizing Offers Made", 0) for t in data)
    upsize_successes = sum(t.get("# of Sucessfull Upsizing chances", 0) for t in data)
    largest_offers = sum(t.get("# of Times largest Option Offered", 0) for t in data)
    
    # Calculate upsizing revenue using actual item prices
    upsize_revenue = 0.0
    for transaction in data:
        upsized_items_field = transaction.get("Items Successfully Upsized", "0")
        if upsized_items_field and upsized_items_field != "0":
            upsized_items = []
            if isinstance(upsized_items_field, str):
                try:
                    import json
                    parsed = json.loads(upsized_items_field)
                    if isinstance(parsed, list):
                        upsized_items = [str(item) for item in parsed]
                    elif isinstance(parsed, dict):
                        upsized_items = list(parsed.keys())
                    else:
                        upsized_items = [str(parsed)]
                except:
                    if upsized_items_field != "0":
                        upsized_items = [upsized_items_field]
            elif isinstance(upsized_items_field, (list, dict)):
                upsized_items = list(upsized_items_field) if isinstance(upsized_items_field, list) else list(upsized_items_field.keys())
            
            for item in upsized_items:
                upsize_revenue += get_item_price(item, item_prices)
    
    upsizing = {
        "total_opportunities": upsize_opportunities,
        "total_offers": upsize_offers,
        "total_successes": upsize_successes,
        "total_revenue": upsize_revenue,
        "avg_revenue_per_success": upsize_revenue / upsize_successes if upsize_successes > 0 else 0,
        "largest_offers": largest_offers,
        "success_rate": (upsize_successes / upsize_offers * 100) if upsize_offers > 0 else 0,
        "offer_rate": (upsize_offers / upsize_opportunities * 100) if upsize_opportunities > 0 else 0,
        "conversion_rate": (upsize_successes / upsize_opportunities * 100) if upsize_opportunities > 0 else 0,
        "largest_offer_rate": (largest_offers / upsize_offers * 100) if upsize_offers > 0 else 0,
    }
    
    # Calculate add-on metrics
    addon_opportunities = sum(t.get("# of Chances to Add-on", 0) for t in data)
    addon_offers = sum(t.get("# of Add-on Offers", 0) for t in data)
    addon_successes = sum(t.get("# of Succesful Add-on Offers", 0) for t in data)
    
    # Calculate add-on revenue using actual item prices
    addon_revenue = 0.0
    for transaction in data:
        addon_items_field = transaction.get("Items with Successful Add-Ons", "0")
        if addon_items_field and addon_items_field != "0":
            addon_items = []
            if isinstance(addon_items_field, str):
                try:
                    import json
                    parsed = json.loads(addon_items_field)
                    if isinstance(parsed, list):
                        addon_items = [str(item) for item in parsed]
                    elif isinstance(parsed, dict):
                        addon_items = list(parsed.keys())
                    else:
                        addon_items = [str(parsed)]
                except:
                    if addon_items_field != "0":
                        addon_items = [addon_items_field]
            elif isinstance(addon_items_field, (list, dict)):
                addon_items = list(addon_items_field) if isinstance(addon_items_field, list) else list(addon_items_field.keys())
            
            for item in addon_items:
                addon_revenue += get_item_price(item, item_prices)
    
    addons = {
        "total_opportunities": addon_opportunities,
        "total_offers": addon_offers,
        "total_successes": addon_successes,
        "total_revenue": addon_revenue,
        "avg_revenue_per_success": addon_revenue / addon_successes if addon_successes > 0 else 0,
        "success_rate": (addon_successes / addon_offers * 100) if addon_offers > 0 else 0,
        "offer_rate": (addon_offers / addon_opportunities * 100) if addon_opportunities > 0 else 0,
        "conversion_rate": (addon_successes / addon_opportunities * 100) if addon_opportunities > 0 else 0,
    }
    
    # Calculate operator analytics with item breakdown
    operator_analytics = {}
    operator_transactions = defaultdict(list)
    
    for transaction in data:
        operator = transaction.get("Operator Name", transaction.get("Operator", "Unknown Operator"))
        operator_transactions[operator].append(transaction)
    
    for operator, op_transactions in operator_transactions.items():
        # Calculate basic metrics for this operator
        op_upsell_opportunities = sum(t.get("# of Chances to Upsell", 0) for t in op_transactions)
        op_upsell_offers = sum(t.get("# of Upselling Offers Made", 0) for t in op_transactions)
        op_upsell_successes = sum(t.get("# of Sucessfull Upselling chances", 0) for t in op_transactions)
        
        op_upsize_opportunities = sum(t.get("# of Chances to Upsize", 0) for t in op_transactions)
        op_upsize_offers = sum(t.get("# of Upsizing Offers Made", 0) for t in op_transactions)
        op_upsize_successes = sum(t.get("# of Sucessfull Upsizing chances", 0) for t in op_transactions)
        
        op_addon_opportunities = sum(t.get("# of Chances to Add-on", 0) for t in op_transactions)
        op_addon_offers = sum(t.get("# of Add-on Offers", 0) for t in op_transactions)
        op_addon_successes = sum(t.get("# of Succesful Add-on Offers", 0) for t in op_transactions)
        
        # Calculate per-item breakdown
        item_performance = defaultdict(lambda: {
            "frequency": 0,
            "overall": {"total_opportunities": 0, "total_offers": 0, "total_successes": 0, "conversion_rate": 0}
        })
        
        for transaction in op_transactions:
            # Parse items initially requested
            items_field = transaction.get("Items Initially Requested", "0")
            initial_items = []
            
            if items_field and items_field != "0":
                if isinstance(items_field, str):
                    try:
                        import json
                        parsed = json.loads(items_field)
                        if isinstance(parsed, list):
                            initial_items = [str(item) for item in parsed]
                        elif isinstance(parsed, dict):
                            initial_items = list(parsed.keys())
                        else:
                            initial_items = [str(parsed)]
                    except:
                        initial_items = [items_field]
                elif isinstance(items_field, (list, dict)):
                    initial_items = list(items_field) if isinstance(items_field, list) else list(items_field.keys())
            
            # Track items
            for item in initial_items:
                stats = item_performance[item]
                stats["frequency"] += 1
                stats["overall"]["total_opportunities"] += (
                    transaction.get("# of Chances to Upsell", 0) +
                    transaction.get("# of Chances to Upsize", 0) +
                    transaction.get("# of Chances to Add-on", 0)
                )
                stats["overall"]["total_offers"] += (
                    transaction.get("# of Upselling Offers Made", 0) +
                    transaction.get("# of Upsizing Offers Made", 0) +
                    transaction.get("# of Add-on Offers", 0)
                )
                stats["overall"]["total_successes"] += (
                    transaction.get("# of Sucessfull Upselling chances", 0) +
                    transaction.get("# of Sucessfull Upsizing chances", 0) +
                    transaction.get("# of Succesful Add-on Offers", 0)
                )
        
        # Calculate conversion rates for items
        for item, stats in item_performance.items():
            if stats["overall"]["total_opportunities"] > 0:
                stats["overall"]["conversion_rate"] = (stats["overall"]["total_successes"] / stats["overall"]["total_opportunities"]) * 100
        
        # Sort items by frequency
        sorted_items = sorted(item_performance.items(), key=lambda x: x[1]["frequency"], reverse=True)
        
        item_breakdown = {
            "items": dict(sorted_items),
            "summary": {
                "total_unique_items": len(item_performance),
                "most_frequent_item": sorted_items[0][0] if sorted_items else None,
                "best_conversion_item": max(item_performance.items(), key=lambda x: x[1]["overall"]["conversion_rate"])[0] if item_performance else None
            }
        }
        
        # Calculate overall performance for this operator
        total_opportunities = op_upsell_opportunities + op_upsize_opportunities + op_addon_opportunities
        total_offers = op_upsell_offers + op_upsize_offers + op_addon_offers
        total_successes = op_upsell_successes + op_upsize_successes + op_addon_successes
        
        # Calculate revenue for this operator using actual item prices
        op_upsell_revenue = 0.0
        op_upsize_revenue = 0.0
        op_addon_revenue = 0.0
        
        for transaction in op_transactions:
            # Upsell revenue
            upsold_items_field = transaction.get("Items Succesfully Upsold", "0")
            if upsold_items_field and upsold_items_field != "0":
                upsold_items = []
                if isinstance(upsold_items_field, str):
                    try:
                        import json
                        parsed = json.loads(upsold_items_field)
                        if isinstance(parsed, list):
                            upsold_items = [str(item) for item in parsed]
                        elif isinstance(parsed, dict):
                            upsold_items = list(parsed.keys())
                        else:
                            upsold_items = [str(parsed)]
                    except:
                        if upsold_items_field != "0":
                            upsold_items = [upsold_items_field]
                elif isinstance(upsold_items_field, (list, dict)):
                    upsold_items = list(upsold_items_field) if isinstance(upsold_items_field, list) else list(upsold_items_field.keys())
                
                for item in upsold_items:
                    op_upsell_revenue += get_item_price(item, item_prices)
            
            # Upsize revenue
            upsized_items_field = transaction.get("Items Successfully Upsized", "0")
            if upsized_items_field and upsized_items_field != "0":
                upsized_items = []
                if isinstance(upsized_items_field, str):
                    try:
                        import json
                        parsed = json.loads(upsized_items_field)
                        if isinstance(parsed, list):
                            upsized_items = [str(item) for item in parsed]
                        elif isinstance(parsed, dict):
                            upsized_items = list(parsed.keys())
                        else:
                            upsized_items = [str(parsed)]
                    except:
                        if upsized_items_field != "0":
                            upsized_items = [upsized_items_field]
                elif isinstance(upsized_items_field, (list, dict)):
                    upsized_items = list(upsized_items_field) if isinstance(upsized_items_field, list) else list(upsized_items_field.keys())
                
                for item in upsized_items:
                    op_upsize_revenue += get_item_price(item, item_prices)
            
            # Add-on revenue
            addon_items_field = transaction.get("Items with Successful Add-Ons", "0")
            if addon_items_field and addon_items_field != "0":
                addon_items = []
                if isinstance(addon_items_field, str):
                    try:
                        import json
                        parsed = json.loads(addon_items_field)
                        if isinstance(parsed, list):
                            addon_items = [str(item) for item in parsed]
                        elif isinstance(parsed, dict):
                            addon_items = list(parsed.keys())
                        else:
                            addon_items = [str(parsed)]
                    except:
                        if addon_items_field != "0":
                            addon_items = [addon_items_field]
                elif isinstance(addon_items_field, (list, dict)):
                    addon_items = list(addon_items_field) if isinstance(addon_items_field, list) else list(addon_items_field.keys())
                
                for item in addon_items:
                    op_addon_revenue += get_item_price(item, item_prices)
        
        total_revenue = op_upsell_revenue + op_upsize_revenue + op_addon_revenue
        
        operator_analytics[operator] = {
            "transaction_count": len(op_transactions),
            "total_opportunities": total_opportunities,
            "total_offers": total_offers,
            "total_successes": total_successes,
            "total_revenue": round(total_revenue, 2),
            "avg_revenue_per_success": round(total_revenue / total_successes, 2) if total_successes > 0 else 0,
            "overall_conversion_rate": (total_successes / total_opportunities * 100) if total_opportunities > 0 else 0,
            "overall_offer_rate": (total_offers / total_opportunities * 100) if total_opportunities > 0 else 0,
            "overall_success_rate": (total_successes / total_offers * 100) if total_offers > 0 else 0,
            "item_breakdown": item_breakdown,
            "upselling": {
                "total_opportunities": op_upsell_opportunities,
                "total_offers": op_upsell_offers,
                "total_successes": op_upsell_successes,
                "total_revenue": round(op_upsell_revenue, 2),
                "avg_revenue_per_success": round(op_upsell_revenue / op_upsell_successes, 2) if op_upsell_successes > 0 else 0,
                "conversion_rate": (op_upsell_successes / op_upsell_opportunities * 100) if op_upsell_opportunities > 0 else 0,
            },
            "upsizing": {
                "total_opportunities": op_upsize_opportunities,
                "total_offers": op_upsize_offers,
                "total_successes": op_upsize_successes,
                "total_revenue": round(op_upsize_revenue, 2),
                "avg_revenue_per_success": round(op_upsize_revenue / op_upsize_successes, 2) if op_upsize_successes > 0 else 0,
                "conversion_rate": (op_upsize_successes / op_upsize_opportunities * 100) if op_upsize_opportunities > 0 else 0,
            },
            "addons": {
                "total_opportunities": op_addon_opportunities,
                "total_offers": op_addon_offers,
                "total_successes": op_addon_successes,
                "total_revenue": round(op_addon_revenue, 2),
                "avg_revenue_per_success": round(op_addon_revenue / op_addon_successes, 2) if op_addon_successes > 0 else 0,
                "conversion_rate": (op_addon_successes / op_addon_opportunities * 100) if op_addon_opportunities > 0 else 0,
            }
        }
    
    # Generate recommendations
    recommendations = []
    if upselling["offer_rate"] < 50:
        recommendations.append(f"🎯 Upselling offer rate is only {upselling['offer_rate']:.1f}%. Train staff to identify and act on more upselling opportunities.")
    if upsizing["offer_rate"] < 60:
        recommendations.append(f"📏 Upsizing offer rate is {upsizing['offer_rate']:.1f}%. Encourage staff to suggest larger sizes more consistently.")
    if upsizing["largest_offer_rate"] < 80:
        recommendations.append(f"⬆️ Only {upsizing['largest_offer_rate']:.1f}% of upsize offers mention the largest option. Train staff to always offer the premium size.")
    if addons["offer_rate"] < 40:
        recommendations.append(f"🍟 Add-on offer rate is {addons['offer_rate']:.1f}%. Focus on suggesting extras like toppings, sides, and premium options.")
    
    return {
        "run_id": run_id,
        "generated_at": datetime.now().isoformat(),
        "success": True,
        "summary": summary,
        "upselling": upselling,
        "upsizing": upsizing,
        "addons": addons,
        "operator_analytics": operator_analytics,
        "recommendations": recommendations
    }

@analytics_bp.route('/recent-runs', methods=['GET'])
def get_recent_runs_analytics():
    """
    Get comprehensive analytics for the most recent runs
    
    Query Parameters:
        limit (int): Number of recent runs to include (default: 5, max: 20)
        
    Returns:
        JSON: List of comprehensive analytics reports for recent runs
    """
    try:
        limit = request.args.get('limit', 5, type=int)
        limit = min(limit, 20)  # Cap at 20 for performance
        
        logger.info(f"Getting analytics for {limit} recent runs")
        
        # Get recent run IDs from the database
        result = db.client.from_('graded_rows_filtered').select('Run ID').execute()
        
        if not result.data:
            return jsonify({
                'success': False,
                'error': 'No runs found in database'
            }), 404
        
        # Get unique run IDs and take the most recent ones
        run_ids = list(set(row['Run ID'] for row in result.data if row.get('Run ID')))
        recent_run_ids = run_ids[:limit]
        
        logger.info(f"Processing run IDs: {recent_run_ids}")
        
        analytics_reports = []
        
        for run_id in recent_run_ids:
            try:
                logger.info(f"Processing run: {run_id}")
                
                # Get data for this run
                run_result = db.client.from_('graded_rows_filtered')\
                    .select('*')\
                    .eq('Run ID', run_id)\
                    .execute()
                
                if run_result.data:
                    # Generate comprehensive analytics report
                    report = generate_analytics_report(run_id, run_result.data)
                    analytics_reports.append(report)
                    logger.info(f"Generated report for {run_id}: {len(run_result.data)} transactions")
                    
            except Exception as e:
                logger.error(f"Error generating analytics for run {run_id}: {e}")
                analytics_reports.append({
                    'run_id': run_id,
                    'success': False,
                    'error': str(e)
                })
        
        logger.info(f"Returning {len(analytics_reports)} reports")
        
        return jsonify({
            'success': True,
            'data': analytics_reports,
            'count': len(analytics_reports),
            'requested_limit': limit
        }), 200
        
    except Exception as e:
        logger.error(f"Error retrieving recent runs analytics: {e}")
        return jsonify({
            'success': False,
            'error': 'Internal server error'
        }), 500

@analytics_bp.route('/run/<run_id>/comprehensive', methods=['GET'])
def get_comprehensive_run_analytics(run_id: str):
    """
    Get comprehensive analytics for a specific run including per-item operator breakdown
    
    Returns:
        JSON: Complete analytics report with all breakdowns
    """
    try:
        logger.info(f"Getting comprehensive analytics for run: {run_id}")
        
        # Get data for this run
        result = db.client.from_('graded_rows_filtered')\
            .select('*')\
            .eq('Run ID', run_id)\
            .execute()
        
        if not result.data:
            return jsonify({
                'success': False,
                'error': f'No data found for run {run_id}'
            }), 404
        
        # Generate comprehensive analytics report
        report = generate_analytics_report(run_id, result.data)
        
        logger.info(f"Generated comprehensive report for {run_id}")
        
        return jsonify({
            'success': True,
            'data': report
        }), 200
        
    except Exception as e:
        logger.error(f"Error generating comprehensive analytics for run {run_id}: {e}")
        return jsonify({
            'success': False,
            'error': 'Internal server error'
        }), 500

@analytics_bp.route('/health', methods=['GET'])
def health_check():
    """Simple health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'message': 'Analytics API is running'
    }), 200