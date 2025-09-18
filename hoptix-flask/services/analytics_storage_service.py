#!/usr/bin/env python3
"""
Simplified Analytics Storage Service for Hoptix

This service handles storing and retrieving analytics results using a single
run_analytics table indexed by run_id, video_id, and operator_name.
All aggregations are done via SQL views and queries.
"""

import json
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime
from integrations.db_supabase import Supa

logger = logging.getLogger(__name__)

class AnalyticsStorageService:
    """Simplified service for storing and retrieving analytics results"""
    
    def __init__(self, db: Supa):
        self.db = db
    
    def store_run_analytics(self, run_id: str, analytics_report: Dict[str, Any]) -> bool:
        """
        Store analytics results for a run by breaking them down into video-operator records
        
        Args:
            run_id: The run ID to store analytics for
            analytics_report: The complete analytics report from HoptixAnalyticsService
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            operator_analytics = analytics_report.get('operator_analytics', {})
            if not operator_analytics:
                logger.warning(f"No operator analytics found for run {run_id}")
                return True
            
            # Collect all operators from different categories
            all_operators = set()
            upsell_by_operator = operator_analytics.get('upselling', {})
            upsize_by_operator = operator_analytics.get('upsizing', {})
            addon_by_operator = operator_analytics.get('addons', {})
            
            all_operators.update(upsell_by_operator.keys())
            all_operators.update(upsize_by_operator.keys())
            all_operators.update(addon_by_operator.keys())
            
            if not all_operators:
                logger.info(f"No operator data found for run {run_id}")
                return True
            
            # Get video IDs for this run
            videos_result = self.db.client.table('videos').select('id').eq('run_id', run_id).execute()
            video_ids = [v['id'] for v in videos_result.data] if videos_result.data else []
            
            if not video_ids:
                logger.warning(f"No videos found for run {run_id}")
                return True
            
            # Create analytics records for each operator
            analytics_records = []
            
            for operator_name in all_operators:
                upsell_data = upsell_by_operator.get(operator_name, {})
                upsize_data = upsize_by_operator.get(operator_name, {})
                addon_data = addon_by_operator.get(operator_name, {})
                
                # Calculate totals for this operator
                total_opportunities = (
                    upsell_data.get('total_opportunities', 0) +
                    upsize_data.get('total_opportunities', 0) +
                    addon_data.get('total_opportunities', 0)
                )
                total_offers = (
                    upsell_data.get('total_offers', 0) +
                    upsize_data.get('total_offers', 0) +
                    addon_data.get('total_offers', 0)
                )
                total_successes = (
                    upsell_data.get('total_successes', 0) +
                    upsize_data.get('total_successes', 0) +
                    addon_data.get('total_successes', 0)
                )
                total_revenue = (
                    upsell_data.get('total_revenue', 0) +
                    upsize_data.get('total_revenue', 0) +
                    addon_data.get('total_revenue', 0)
                )
                
                # For simplicity, create one aggregated record per operator for the run
                # Use the first video_id as a placeholder (in real implementation, you'd have multiple records)
                analytics_record = {
                    'run_id': run_id,
                    'video_id': video_ids[0],  # Placeholder - in reality you'd have one record per video
                    'operator_name': operator_name,
                    
                    # Transaction counts (would come from actual transaction data)
                    'total_transactions': 0,  # TODO: Calculate from actual data
                    'complete_transactions': 0,  # TODO: Calculate from actual data
                    
                    # Overall performance
                    'total_opportunities': total_opportunities,
                    'total_offers': total_offers,
                    'total_successes': total_successes,
                    'total_revenue': round(total_revenue, 2),
                    
                    # Upselling metrics
                    'upsell_opportunities': upsell_data.get('total_opportunities', 0),
                    'upsell_offers': upsell_data.get('total_offers', 0),
                    'upsell_successes': upsell_data.get('total_successes', 0),
                    'upsell_revenue': round(upsell_data.get('total_revenue', 0), 2),
                    
                    # Upsizing metrics
                    'upsize_opportunities': upsize_data.get('total_opportunities', 0),
                    'upsize_offers': upsize_data.get('total_offers', 0),
                    'upsize_successes': upsize_data.get('total_successes', 0),
                    'upsize_revenue': round(upsize_data.get('total_revenue', 0), 2),
                    'largest_offers': upsize_data.get('largest_offers', 0),
                    
                    # Add-on metrics
                    'addon_opportunities': addon_data.get('total_opportunities', 0),
                    'addon_offers': addon_data.get('total_offers', 0),
                    'addon_successes': addon_data.get('total_successes', 0),
                    'addon_revenue': round(addon_data.get('total_revenue', 0), 2),
                    
                    # Item counts (approximations)
                    'items_initial': 0,  # TODO: Calculate from actual data
                    'items_final': 0,   # TODO: Calculate from actual data
                    'successful_items': total_successes,  # Approximation
                }
                
                analytics_records.append(analytics_record)
            
            # Upsert all analytics records
            if analytics_records:
                result = self.db.client.table('run_analytics').upsert(
                    analytics_records,
                    on_conflict='run_id,video_id,operator_name'
                ).execute()
                
                if result.data:
                    logger.info(f"Successfully stored {len(analytics_records)} analytics records for run {run_id}")
                    return True
                else:
                    logger.error(f"Failed to store analytics records for run {run_id}: No data returned")
                    return False
            
            return True
            
        except Exception as e:
            logger.error(f"Error storing analytics for run {run_id}: {e}")
            return False
    
    def get_run_analytics(self, run_id: str) -> Optional[Dict[str, Any]]:
        """Get aggregated run totals (for backward compatibility)"""
        return self.get_run_totals(run_id)
    
    def get_run_totals(self, run_id: str) -> Optional[Dict[str, Any]]:
        """Get aggregated run totals"""
        try:
            result = self.db.client.table('run_analytics_with_details')\
                .select('*')\
                .eq('run_id', run_id)\
                .limit(1)\
                .execute()
            
            return result.data[0] if result.data else None
            
        except Exception as e:
            logger.error(f"Error retrieving run totals for {run_id}: {e}")
            return None
    
    def get_operator_performance_by_run(self, run_id: str) -> List[Dict[str, Any]]:
        """Get operator performance for a specific run"""
        try:
            result = self.db.client.table('operator_performance_by_run')\
                .select('*')\
                .eq('run_id', run_id)\
                .order('overall_conversion_rate', desc=True)\
                .execute()
            
            return result.data if result.data else []
            
        except Exception as e:
            logger.error(f"Error retrieving operator performance for run {run_id}: {e}")
            return []
    
    def get_location_analytics(self, location_id: str, limit: int = 10) -> List[Dict[str, Any]]:
        """Get recent run totals for a location"""
        try:
            result = self.db.client.table('run_analytics_with_details')\
                .select('*')\
                .eq('location_id', location_id)\
                .order('run_date', desc=True)\
                .limit(limit)\
                .execute()
            
            return result.data if result.data else []
            
        except Exception as e:
            logger.error(f"Error retrieving location analytics for {location_id}: {e}")
            return []
    
    def get_operator_performance_by_location(self, location_id: str, days: int = 30) -> List[Dict[str, Any]]:
        """Get operator performance for a location over time"""
        try:
            result = self.db.client.table('operator_performance_by_run')\
                .select('*')\
                .eq('location_id', location_id)\
                .gte('run_date', f'now() - interval \'{days} days\'')\
                .order('run_date', desc=True)\
                .execute()
            
            return result.data if result.data else []
            
        except Exception as e:
            logger.error(f"Error retrieving operator performance for location {location_id}: {e}")
            return []
    
    def get_analytics_trends(self, location_id: str, days: int = 30) -> List[Dict[str, Any]]:
        """Get analytics trends over time for a location"""
        try:
            result = self.db.client.table('run_analytics_with_details')\
                .select('run_date, completion_rate, overall_conversion_rate, total_revenue, total_transactions')\
                .eq('location_id', location_id)\
                .gte('run_date', f'now() - interval \'{days} days\'')\
                .order('run_date', desc=False)\
                .execute()
            
            return result.data if result.data else []
            
        except Exception as e:
            logger.error(f"Error retrieving analytics trends for {location_id}: {e}")
            return []
    
    def get_org_analytics_summary(self, org_id: str, days: int = 30) -> Dict[str, Any]:
        """Get aggregated analytics summary for an organization"""
        try:
            # Get all runs for the org in the time period
            result = self.db.client.table('run_analytics_with_details')\
                .select('*')\
                .eq('org_id', org_id)\
                .gte('run_date', f'now() - interval \'{days} days\'')\
                .execute()
            
            if not result.data:
                return {}
            
            # Aggregate across all runs
            totals = {
                'total_runs': len(result.data),
                'total_transactions': sum(r.get('total_transactions', 0) for r in result.data),
                'total_opportunities': sum(r.get('total_opportunities', 0) for r in result.data),
                'total_successes': sum(r.get('total_successes', 0) for r in result.data),
                'total_revenue': sum(r.get('total_revenue', 0) for r in result.data),
            }
            
            # Calculate averages
            totals['avg_conversion_rate'] = (
                totals['total_successes'] / totals['total_opportunities'] * 100 
                if totals['total_opportunities'] > 0 else 0
            )
            
            return totals
            
        except Exception as e:
            logger.error(f"Error retrieving org summary for {org_id}: {e}")
            return {}