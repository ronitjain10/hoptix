#!/usr/bin/env python3
"""
Analytics API Routes for Hoptix

Provides REST API endpoints for accessing stored analytics data.
"""

from flask import Blueprint, request, jsonify
from integrations.db_supabase import Supa
from services.analytics_storage_service import AnalyticsStorageService
from config import Settings
import logging

logger = logging.getLogger(__name__)

# Create blueprint
analytics_bp = Blueprint('analytics', __name__, url_prefix='/api/analytics')

# Initialize services
settings = Settings()
db = Supa(settings.SUPABASE_URL, settings.SUPABASE_SERVICE_KEY)
storage_service = AnalyticsStorageService(db)

@analytics_bp.route('/run/<run_id>', methods=['GET'])
def get_run_analytics(run_id: str):
    """
    Get analytics results for a specific run
    
    Returns:
        JSON: Analytics data for the run
    """
    try:
        analytics = storage_service.get_run_analytics(run_id)
        
        if analytics:
            return jsonify({
                'success': True,
                'data': analytics
            }), 200
        else:
            return jsonify({
                'success': False,
                'error': 'Analytics not found for this run'
            }), 404
            
    except Exception as e:
        logger.error(f"Error retrieving run analytics: {e}")
        return jsonify({
            'success': False,
            'error': 'Internal server error'
        }), 500

@analytics_bp.route('/location/<location_id>', methods=['GET'])
def get_location_analytics(location_id: str):
    """
    Get recent analytics results for a location
    
    Query Parameters:
        limit (int): Maximum number of results (default: 10)
        
    Returns:
        JSON: List of analytics results for the location
    """
    try:
        limit = request.args.get('limit', 10, type=int)
        limit = min(limit, 100)  # Cap at 100 for performance
        
        analytics_list = storage_service.get_location_analytics(location_id, limit)
        
        return jsonify({
            'success': True,
            'data': analytics_list,
            'count': len(analytics_list)
        }), 200
        
    except Exception as e:
        logger.error(f"Error retrieving location analytics: {e}")
        return jsonify({
            'success': False,
            'error': 'Internal server error'
        }), 500

@analytics_bp.route('/organization/<org_id>/summary', methods=['GET'])
def get_org_summary(org_id: str):
    """
    Get aggregated analytics summary for an organization
    
    Query Parameters:
        days (int): Number of days to look back (default: 30)
        
    Returns:
        JSON: Aggregated analytics summary
    """
    try:
        days = request.args.get('days', 30, type=int)
        days = min(days, 365)  # Cap at 1 year
        
        summary = storage_service.get_org_analytics_summary(org_id, days)
        
        return jsonify({
            'success': True,
            'data': summary
        }), 200
        
    except Exception as e:
        logger.error(f"Error retrieving org summary: {e}")
        return jsonify({
            'success': False,
            'error': 'Internal server error'
        }), 500

@analytics_bp.route('/location/<location_id>/trends', methods=['GET'])
def get_location_trends(location_id: str):
    """
    Get analytics trends over time for a location
    
    Query Parameters:
        days (int): Number of days to look back (default: 30)
        
    Returns:
        JSON: Time series data for trending charts
    """
    try:
        days = request.args.get('days', 30, type=int)
        days = min(days, 365)  # Cap at 1 year
        
        trends = storage_service.get_analytics_trends(location_id, days)
        
        return jsonify({
            'success': True,
            'data': trends,
            'period_days': days
        }), 200
        
    except Exception as e:
        logger.error(f"Error retrieving location trends: {e}")
        return jsonify({
            'success': False,
            'error': 'Internal server error'
        }), 500

@analytics_bp.route('/run/<run_id>/detailed', methods=['GET'])
def get_run_detailed_analytics(run_id: str):
    """
    Get detailed analytics (including operator breakdowns) for a specific run
    
    Returns:
        JSON: Detailed analytics including item-by-item and operator-by-operator data
    """
    try:
        analytics = storage_service.get_run_analytics(run_id)
        
        if not analytics:
            return jsonify({
                'success': False,
                'error': 'Analytics not found for this run'
            }), 404
        
        # Extract detailed analytics from the stored JSON
        detailed_analytics = analytics.get('detailed_analytics', {})
        
        # Structure the response for frontend consumption
        response_data = {
            'run_id': run_id,
            'run_date': analytics.get('run_date'),
            'location_id': analytics.get('location_id'),
            'org_id': analytics.get('org_id'),
            
            # Summary metrics
            'summary': {
                'total_transactions': analytics.get('total_transactions', 0),
                'complete_transactions': analytics.get('complete_transactions', 0),
                'completion_rate': analytics.get('completion_rate', 0),
                'total_revenue': analytics.get('total_revenue', 0),
                'overall_conversion_rate': analytics.get('overall_conversion_rate', 0)
            },
            
            # Category performance
            'categories': {
                'upselling': {
                    'opportunities': analytics.get('upsell_opportunities', 0),
                    'offers': analytics.get('upsell_offers', 0),
                    'successes': analytics.get('upsell_successes', 0),
                    'conversion_rate': analytics.get('upsell_conversion_rate', 0),
                    'revenue': analytics.get('upsell_revenue', 0)
                },
                'upsizing': {
                    'opportunities': analytics.get('upsize_opportunities', 0),
                    'offers': analytics.get('upsize_offers', 0),
                    'successes': analytics.get('upsize_successes', 0),
                    'conversion_rate': analytics.get('upsize_conversion_rate', 0),
                    'revenue': analytics.get('upsize_revenue', 0)
                },
                'addons': {
                    'opportunities': analytics.get('addon_opportunities', 0),
                    'offers': analytics.get('addon_offers', 0),
                    'successes': analytics.get('addon_successes', 0),
                    'conversion_rate': analytics.get('addon_conversion_rate', 0),
                    'revenue': analytics.get('addon_revenue', 0)
                }
            },
            
            # Detailed breakdowns (if available)
            'item_breakdowns': {
                'upselling': detailed_analytics.get('upselling', {}).get('by_item', {}),
                'upsizing': detailed_analytics.get('upsizing', {}).get('by_item', {}),
                'addons': detailed_analytics.get('addons', {}).get('by_item', {})
            },
            
            # Operator performance (if available)
            'operator_analytics': detailed_analytics.get('operator_analytics', {}),
            
            # Recommendations
            'recommendations': detailed_analytics.get('recommendations', [])
        }
        
        return jsonify({
            'success': True,
            'data': response_data
        }), 200
        
    except Exception as e:
        logger.error(f"Error retrieving detailed run analytics: {e}")
        return jsonify({
            'success': False,
            'error': 'Internal server error'
        }), 500

@analytics_bp.route('/location/<location_id>/leaderboard', methods=['GET'])
def get_location_leaderboard(location_id: str):
    """
    Get operator performance leaderboard for a location
    
    Query Parameters:
        days (int): Number of days to look back (default: 30)
        metric (str): Metric to rank by (conversion_rate, revenue, total_successes)
        
    Returns:
        JSON: Operator rankings
    """
    try:
        days = request.args.get('days', 30, type=int)
        metric = request.args.get('metric', 'conversion_rate')
        
        # Get recent analytics for this location
        analytics_list = storage_service.get_location_analytics(location_id, 50)  # Get more for aggregation
        
        # Aggregate operator performance across runs
        operator_stats = {}
        
        for analytics in analytics_list:
            detailed = analytics.get('detailed_analytics', {})
            operator_analytics = detailed.get('operator_analytics', {})
            
            # Process each category
            for category in ['upselling', 'upsizing', 'addons']:
                category_data = operator_analytics.get(category, {})
                
                for operator, metrics in category_data.items():
                    if operator not in operator_stats:
                        operator_stats[operator] = {
                            'total_opportunities': 0,
                            'total_offers': 0,
                            'total_successes': 0,
                            'total_revenue': 0
                        }
                    
                    stats = operator_stats[operator]
                    stats['total_opportunities'] += metrics.get('total_opportunities', 0)
                    stats['total_offers'] += metrics.get('total_offers', 0)
                    stats['total_successes'] += metrics.get('total_successes', 0)
                    stats['total_revenue'] += metrics.get('total_revenue', 0)
        
        # Calculate final metrics and rank
        leaderboard = []
        for operator, stats in operator_stats.items():
            conversion_rate = (stats['total_successes'] / stats['total_opportunities'] * 100) if stats['total_opportunities'] > 0 else 0
            
            leaderboard.append({
                'operator': operator,
                'conversion_rate': round(conversion_rate, 2),
                'total_revenue': round(stats['total_revenue'], 2),
                'total_successes': stats['total_successes'],
                'total_opportunities': stats['total_opportunities'],
                'total_offers': stats['total_offers']
            })
        
        # Sort by requested metric
        if metric == 'conversion_rate':
            leaderboard.sort(key=lambda x: x['conversion_rate'], reverse=True)
        elif metric == 'revenue':
            leaderboard.sort(key=lambda x: x['total_revenue'], reverse=True)
        elif metric == 'total_successes':
            leaderboard.sort(key=lambda x: x['total_successes'], reverse=True)
        
        return jsonify({
            'success': True,
            'data': leaderboard[:10],  # Top 10
            'metric': metric,
            'period_days': days
        }), 200
        
    except Exception as e:
        logger.error(f"Error retrieving location leaderboard: {e}")
        return jsonify({
            'success': False,
            'error': 'Internal server error'
        }), 500

@analytics_bp.route('/run/<run_id>/operators', methods=['GET'])
def get_run_operator_performance(run_id: str):
    """
    Get operator performance data for a specific run
    
    Returns:
        JSON: List of operator performance records for the run
    """
    try:
        operator_data = storage_service.get_operator_performance_by_run(run_id)
        
        return jsonify({
            'success': True,
            'data': operator_data,
            'count': len(operator_data)
        }), 200
        
    except Exception as e:
        logger.error(f"Error retrieving run operator performance: {e}")
        return jsonify({
            'success': False,
            'error': 'Internal server error'
        }), 500

@analytics_bp.route('/location/<location_id>/operators', methods=['GET'])
def get_location_operator_performance(location_id: str):
    """
    Get operator performance data for a location
    
    Query Parameters:
        days (int): Number of days to look back (default: 30)
        
    Returns:
        JSON: List of operator performance records for the location
    """
    try:
        days = request.args.get('days', 30, type=int)
        days = min(days, 365)  # Cap at 1 year
        
        operator_data = storage_service.get_operator_performance_by_location(location_id, days)
        
        return jsonify({
            'success': True,
            'data': operator_data,
            'count': len(operator_data),
            'period_days': days
        }), 200
        
    except Exception as e:
        logger.error(f"Error retrieving location operator performance: {e}")
        return jsonify({
            'success': False,
            'error': 'Internal server error'
        }), 500
