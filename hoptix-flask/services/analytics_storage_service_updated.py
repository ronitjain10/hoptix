#!/usr/bin/env python3
"""
Simplified Analytics Storage Service for Hoptix

This service now just provides data access methods for analytics.
All calculations are done on-demand from the graded_rows_filtered view.
No more storing pre-calculated analytics - everything is calculated fresh.
"""

import logging
from typing import Dict, List, Any, Optional
from datetime import datetime
from integrations.db_supabase import Supa
from .analytics_service_updated import HoptixAnalyticsService

logger = logging.getLogger(__name__)

class AnalyticsStorageService:
    """Simplified service for retrieving analytics data from graded_rows_filtered view"""
    
    def __init__(self, db: Supa):
        self.db = db
        self.analytics_service = HoptixAnalyticsService(db)
    
    def get_run_analytics(self, run_id: str) -> Optional[Dict[str, Any]]:
        """Get comprehensive analytics for a specific run"""
        try:
            return self.analytics_service.generate_run_report(run_id)
        except Exception as e:
            logger.error(f"Error retrieving run analytics for {run_id}: {e}")
            return None
    
    def get_run_totals(self, run_id: str) -> Optional[Dict[str, Any]]:
        """Get aggregated run totals"""
        try:
            report = self.analytics_service.generate_run_report(run_id)
            if not report:
                return None
            
            # Extract just the summary totals for backward compatibility
            summary = report.get("summary", {})
            upselling = report.get("upselling", {})
            upsizing = report.get("upsizing", {})
            addons = report.get("addons", {})
            
            return {
                "run_id": run_id,
                "total_transactions": summary.get("total_transactions", 0),
                "complete_transactions": summary.get("complete_transactions", 0),
                "completion_rate": summary.get("completion_rate", 0),
                "avg_items_initial": summary.get("avg_items_initial", 0),
                "avg_items_final": summary.get("avg_items_final", 0),
                "avg_item_increase": summary.get("avg_item_increase", 0),
                
                # Upselling totals
                "upsell_opportunities": upselling.get("total_opportunities", 0),
                "upsell_offers": upselling.get("total_offers", 0),
                "upsell_successes": upselling.get("total_successes", 0),
                "upsell_conversion_rate": upselling.get("conversion_rate", 0),
                "upsell_revenue": upselling.get("total_revenue", 0),
                
                # Upsizing totals
                "upsize_opportunities": upsizing.get("total_opportunities", 0),
                "upsize_offers": upsizing.get("total_offers", 0),
                "upsize_successes": upsizing.get("total_successes", 0),
                "upsize_conversion_rate": upsizing.get("conversion_rate", 0),
                "upsize_revenue": upsizing.get("total_revenue", 0),
                
                # Add-on totals
                "addon_opportunities": addons.get("total_opportunities", 0),
                "addon_offers": addons.get("total_offers", 0),
                "addon_successes": addons.get("total_successes", 0),
                "addon_conversion_rate": addons.get("conversion_rate", 0),
                "addon_revenue": addons.get("total_revenue", 0),
                
                # Overall totals
                "total_opportunities": (
                    upselling.get("total_opportunities", 0) +
                    upsizing.get("total_opportunities", 0) +
                    addons.get("total_opportunities", 0)
                ),
                "total_offers": (
                    upselling.get("total_offers", 0) +
                    upsizing.get("total_offers", 0) +
                    addons.get("total_offers", 0)
                ),
                "total_successes": (
                    upselling.get("total_successes", 0) +
                    upsizing.get("total_successes", 0) +
                    addons.get("total_successes", 0)
                ),
                "total_revenue": (
                    upselling.get("total_revenue", 0) +
                    upsizing.get("total_revenue", 0) +
                    addons.get("total_revenue", 0)
                ),
                "overall_conversion_rate": 0,  # Will be calculated below
                
                "created_at": datetime.now().isoformat(),
                "updated_at": datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error retrieving run totals for {run_id}: {e}")
            return None
    
    def get_operator_performance_by_run(self, run_id: str) -> List[Dict[str, Any]]:
        """Get operator performance for a specific run"""
        try:
            return self.analytics_service.get_operator_performance_by_run(run_id)
        except Exception as e:
            logger.error(f"Error retrieving operator performance for run {run_id}: {e}")
            return []
    
    def get_location_analytics(self, location_id: str, limit: int = 10) -> List[Dict[str, Any]]:
        """Get recent run analytics for a location"""
        try:
            # Get recent runs for this location
            runs_result = self.db.client.table('runs')\
                .select('id, run_date')\
                .eq('location_id', location_id)\
                .order('run_date', desc=True)\
                .limit(limit)\
                .execute()
            
            if not runs_result.data:
                return []
            
            # Generate analytics for each run
            location_analytics = []
            for run in runs_result.data:
                run_analytics = self.get_run_totals(run['id'])
                if run_analytics:
                    run_analytics['run_date'] = run['run_date']
                    location_analytics.append(run_analytics)
            
            return location_analytics
            
        except Exception as e:
            logger.error(f"Error retrieving location analytics for {location_id}: {e}")
            return []
    
    def get_operator_performance_by_location(self, location_id: str, days: int = 30) -> List[Dict[str, Any]]:
        """Get operator performance for a location over time"""
        try:
            report = self.analytics_service.generate_location_report(location_id, days)
            if not report or "operator_analytics" not in report:
                return []
            
            # Extract operator performance data
            operator_performance = []
            
            # Get all unique operators across all categories
            all_operators = set()
            upselling_ops = report["operator_analytics"].get("upselling", {})
            upsizing_ops = report["operator_analytics"].get("upsizing", {})
            addon_ops = report["operator_analytics"].get("addons", {})
            
            all_operators.update(upselling_ops.keys())
            all_operators.update(upsizing_ops.keys())
            all_operators.update(addon_ops.keys())
            
            for operator in all_operators:
                upsell_data = upselling_ops.get(operator, {})
                upsize_data = upsizing_ops.get(operator, {})
                addon_data = addon_ops.get(operator, {})
                
                performance = {
                    "operator_name": operator,
                    "location_id": location_id,
                    "period_days": days,
                    
                    # Upselling metrics
                    "upsell_opportunities": upsell_data.get("total_opportunities", 0),
                    "upsell_offers": upsell_data.get("total_offers", 0),
                    "upsell_successes": upsell_data.get("total_successes", 0),
                    "upsell_conversion_rate": upsell_data.get("conversion_rate", 0),
                    "upsell_revenue": upsell_data.get("total_revenue", 0),
                    
                    # Upsizing metrics
                    "upsize_opportunities": upsize_data.get("total_opportunities", 0),
                    "upsize_offers": upsize_data.get("total_offers", 0),
                    "upsize_successes": upsize_data.get("total_successes", 0),
                    "upsize_conversion_rate": upsize_data.get("conversion_rate", 0),
                    "upsize_revenue": upsize_data.get("total_revenue", 0),
                    
                    # Add-on metrics
                    "addon_opportunities": addon_data.get("total_opportunities", 0),
                    "addon_offers": addon_data.get("total_offers", 0),
                    "addon_successes": addon_data.get("total_successes", 0),
                    "addon_conversion_rate": addon_data.get("conversion_rate", 0),
                    "addon_revenue": addon_data.get("total_revenue", 0),
                    
                    # Overall metrics
                    "total_opportunities": (
                        upsell_data.get("total_opportunities", 0) +
                        upsize_data.get("total_opportunities", 0) +
                        addon_data.get("total_opportunities", 0)
                    ),
                    "total_successes": (
                        upsell_data.get("total_successes", 0) +
                        upsize_data.get("total_successes", 0) +
                        addon_data.get("total_successes", 0)
                    ),
                    "total_revenue": (
                        upsell_data.get("total_revenue", 0) +
                        upsize_data.get("total_revenue", 0) +
                        addon_data.get("total_revenue", 0)
                    )
                }
                
                # Calculate overall conversion rate
                performance["overall_conversion_rate"] = (
                    performance["total_successes"] / performance["total_opportunities"] * 100
                    if performance["total_opportunities"] > 0 else 0
                )
                
                operator_performance.append(performance)
            
            # Sort by overall conversion rate
            operator_performance.sort(key=lambda x: x["overall_conversion_rate"], reverse=True)
            
            return operator_performance
            
        except Exception as e:
            logger.error(f"Error retrieving operator performance for location {location_id}: {e}")
            return []
    
    def get_analytics_trends(self, location_id: str, days: int = 30) -> List[Dict[str, Any]]:
        """Get analytics trends over time for a location"""
        try:
            # Get runs for the location in the time period
            runs_result = self.db.client.table('runs')\
                .select('id, run_date')\
                .eq('location_id', location_id)\
                .gte('run_date', f'now() - interval \'{days} days\'')\
                .order('run_date', desc=False)\
                .execute()
            
            if not runs_result.data:
                return []
            
            # Generate analytics for each run date
            trends = []
            for run in runs_result.data:
                run_totals = self.get_run_totals(run['id'])
                if run_totals:
                    trend_data = {
                        "run_date": run['run_date'],
                        "completion_rate": run_totals.get("completion_rate", 0),
                        "overall_conversion_rate": run_totals.get("overall_conversion_rate", 0),
                        "total_revenue": run_totals.get("total_revenue", 0),
                        "total_transactions": run_totals.get("total_transactions", 0)
                    }
                    trends.append(trend_data)
            
            return trends
            
        except Exception as e:
            logger.error(f"Error retrieving analytics trends for {location_id}: {e}")
            return []
    
    def get_org_analytics_summary(self, org_id: str, days: int = 30) -> Dict[str, Any]:
        """Get aggregated analytics summary for an organization"""
        try:
            # Get all locations for the org
            locations_result = self.db.client.table('locations')\
                .select('id')\
                .eq('org_id', org_id)\
                .execute()
            
            if not locations_result.data:
                return {}
            
            location_ids = [loc['id'] for loc in locations_result.data]
            
            # Get all runs for these locations
            runs_result = self.db.client.table('runs')\
                .select('id')\
                .in_('location_id', location_ids)\
                .gte('run_date', f'now() - interval \'{days} days\'')\
                .execute()
            
            if not runs_result.data:
                return {}
            
            # Aggregate analytics across all runs
            total_runs = len(runs_result.data)
            totals = {
                'total_runs': total_runs,
                'total_transactions': 0,
                'total_opportunities': 0,
                'total_successes': 0,
                'total_revenue': 0,
            }
            
            for run in runs_result.data:
                run_totals = self.get_run_totals(run['id'])
                if run_totals:
                    totals['total_transactions'] += run_totals.get('total_transactions', 0)
                    totals['total_opportunities'] += run_totals.get('total_opportunities', 0)
                    totals['total_successes'] += run_totals.get('total_successes', 0)
                    totals['total_revenue'] += run_totals.get('total_revenue', 0)
            
            # Calculate averages
            totals['avg_conversion_rate'] = (
                totals['total_successes'] / totals['total_opportunities'] * 100 
                if totals['total_opportunities'] > 0 else 0
            )
            
            return totals
            
        except Exception as e:
            logger.error(f"Error retrieving org summary for {org_id}: {e}")
            return {}
    
    # Legacy method - no longer stores analytics, just returns success
    def store_run_analytics(self, run_id: str, analytics_report: Dict[str, Any]) -> bool:
        """
        Legacy method for backward compatibility.
        Analytics are now calculated on-demand, so this just returns True.
        """
        logger.info(f"Analytics storage is now on-demand. Skipping storage for run {run_id}")
        return True
