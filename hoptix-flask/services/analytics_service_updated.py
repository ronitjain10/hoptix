#!/usr/bin/env python3
"""
Updated Analytics Service for Hoptix Grading Data

This service calculates analytics on-demand from the graded_rows_filtered view,
which includes all transaction, grading, and worker information in one place.
"""

import json
from typing import Dict, List, Any, Optional, Tuple
from collections import defaultdict, Counter
from datetime import datetime
import logging
from .item_lookup_service import get_item_lookup_service
from integrations.db_supabase import Supa

logger = logging.getLogger(__name__)

class UpsellAnalytics:
    """Handles upselling analytics calculations"""
    
    @staticmethod
    def calculate_upsell_metrics(transactions: List[Dict], item_lookup=None) -> Dict[str, Any]:
        """Calculate comprehensive upsell metrics with revenue tracking"""
        total_opportunities = 0
        total_offers = 0
        total_successes = 0
        total_revenue = 0.0
        
        # Item-specific tracking
        item_opportunities = defaultdict(int)
        item_offers = defaultdict(int)
        item_successes = defaultdict(int)
        item_revenue = defaultdict(float)
        
        # Upsell type tracking
        upsell_items_counter = Counter()
        
        for transaction in transactions:
            # Parse items initially requested
            initial_items = UpsellAnalytics._parse_items_field(transaction.get("Items Initially Requested", "0"))
            
            # Get upsell metrics
            opportunities = transaction.get("# of Chances to Upsell", 0)
            offers = transaction.get("# of Upselling Offers Made", 0)
            successes = transaction.get("# of Sucessfull Upselling chances", 0)
            
            total_opportunities += opportunities
            total_offers += offers
            total_successes += successes
            
            # Count successfully upsold items and calculate revenue
            upsold_items = UpsellAnalytics._parse_items_field(transaction.get("Items Succesfully Upsold", "0"))
            transaction_revenue = 0
            
            for item in upsold_items:
                upsell_items_counter[item] += 1
                
                # Calculate revenue if item_lookup is provided
                if item_lookup:
                    price = item_lookup.get_item_price(item)
                    total_revenue += price
                    transaction_revenue += price
            
            # Track by initial items and distribute revenue proportionally
            for item in initial_items:
                item_opportunities[item] += opportunities
                item_offers[item] += offers
                item_successes[item] += successes
                # Distribute transaction revenue among initial items
                if len(initial_items) > 0:
                    item_revenue[item] += transaction_revenue / len(initial_items)
        
        return {
            "total_opportunities": total_opportunities,
            "total_offers": total_offers,
            "total_successes": total_successes,
            "total_revenue": total_revenue,
            "avg_revenue_per_success": total_revenue / total_successes if total_successes > 0 else 0,
            "success_rate": (total_successes / total_offers * 100) if total_offers > 0 else 0,
            "offer_rate": (total_offers / total_opportunities * 100) if total_opportunities > 0 else 0,
            "conversion_rate": (total_successes / total_opportunities * 100) if total_opportunities > 0 else 0,
            "by_item": {
                item: {
                    "opportunities": item_opportunities[item],
                    "offers": item_offers[item],
                    "successes": item_successes[item],
                    "revenue": item_revenue[item],
                    "success_rate": (item_successes[item] / item_offers[item] * 100) if item_offers[item] > 0 else 0,
                    "offer_rate": (item_offers[item] / item_opportunities[item] * 100) if item_opportunities[item] > 0 else 0
                }
                for item in set(list(item_opportunities.keys()) + list(item_offers.keys()) + list(item_successes.keys()))
            },
            "most_upsold_items": dict(upsell_items_counter.most_common(10))
        }
    
    @staticmethod
    def calculate_upsell_metrics_by_operator(transactions: List[Dict], item_lookup=None) -> Dict[str, Any]:
        """Calculate upsell metrics broken down by operator"""
        operator_metrics = defaultdict(lambda: {
            "total_opportunities": 0,
            "total_offers": 0,
            "total_successes": 0,
            "total_revenue": 0.0,
            "item_opportunities": defaultdict(int),
            "item_offers": defaultdict(int),
            "item_successes": defaultdict(int),
            "item_revenue": defaultdict(float),
            "upsell_items_counter": Counter()
        })
        
        for transaction in transactions:
            # Try both possible operator field names for compatibility
            operator = transaction.get("Operator Name", transaction.get("Operator", "Unknown Operator"))
            
            # Parse items initially requested
            initial_items = UpsellAnalytics._parse_items_field(transaction.get("Items Initially Requested", "0"))
            
            # Get upsell metrics
            opportunities = transaction.get("# of Chances to Upsell", 0)
            offers = transaction.get("# of Upselling Offers Made", 0)
            successes = transaction.get("# of Sucessfull Upselling chances", 0)
            
            operator_data = operator_metrics[operator]
            operator_data["total_opportunities"] += opportunities
            operator_data["total_offers"] += offers
            operator_data["total_successes"] += successes
            
            # Count successfully upsold items and calculate revenue
            upsold_items = UpsellAnalytics._parse_items_field(transaction.get("Items Succesfully Upsold", "0"))
            transaction_revenue = 0
            
            for item in upsold_items:
                operator_data["upsell_items_counter"][item] += 1
                
                # Calculate revenue if item_lookup is provided
                if item_lookup:
                    price = item_lookup.get_item_price(item)
                    operator_data["total_revenue"] += price
                    transaction_revenue += price
            
            # Track by initial items and distribute revenue proportionally
            for item in initial_items:
                operator_data["item_opportunities"][item] += opportunities
                operator_data["item_offers"][item] += offers
                operator_data["item_successes"][item] += successes
                # Distribute transaction revenue among initial items
                if len(initial_items) > 0:
                    operator_data["item_revenue"][item] += transaction_revenue / len(initial_items)
        
        # Convert to final format
        result = {}
        for operator, data in operator_metrics.items():
            result[operator] = {
                "total_opportunities": data["total_opportunities"],
                "total_offers": data["total_offers"],
                "total_successes": data["total_successes"],
                "total_revenue": data["total_revenue"],
                "avg_revenue_per_success": data["total_revenue"] / data["total_successes"] if data["total_successes"] > 0 else 0,
                "success_rate": (data["total_successes"] / data["total_offers"] * 100) if data["total_offers"] > 0 else 0,
                "offer_rate": (data["total_offers"] / data["total_opportunities"] * 100) if data["total_opportunities"] > 0 else 0,
                "conversion_rate": (data["total_successes"] / data["total_opportunities"] * 100) if data["total_opportunities"] > 0 else 0,
                "by_item": {
                    item: {
                        "opportunities": data["item_opportunities"][item],
                        "offers": data["item_offers"][item],
                        "successes": data["item_successes"][item],
                        "revenue": data["item_revenue"][item],
                        "success_rate": (data["item_successes"][item] / data["item_offers"][item] * 100) if data["item_offers"][item] > 0 else 0,
                        "offer_rate": (data["item_offers"][item] / data["item_opportunities"][item] * 100) if data["item_opportunities"][item] > 0 else 0
                    }
                    for item in set(list(data["item_opportunities"].keys()) + list(data["item_offers"].keys()) + list(data["item_successes"].keys()))
                },
                "most_upsold_items": dict(data["upsell_items_counter"].most_common(10))
            }
        
        return result
    
    @staticmethod
    def _parse_items_field(items_field: Any) -> List[str]:
        """Parse items field which can be string, list, or dict"""
        if not items_field or items_field == "0":
            return []
        
        if isinstance(items_field, str):
            try:
                # Try to parse as JSON
                parsed = json.loads(items_field)
                if isinstance(parsed, list):
                    return [str(item) for item in parsed]
                elif isinstance(parsed, dict):
                    return list(parsed.keys())
                else:
                    return [str(parsed)]
            except json.JSONDecodeError:
                # Not JSON, treat as single item
                return [items_field] if items_field != "0" else []
        
        elif isinstance(items_field, list):
            return [str(item) for item in items_field]
        
        elif isinstance(items_field, dict):
            return list(items_field.keys())
        
        else:
            return [str(items_field)]


class UpsizeAnalytics:
    """Handles upsizing analytics calculations"""
    
    @staticmethod
    def calculate_upsize_metrics(transactions: List[Dict], item_lookup=None) -> Dict[str, Any]:
        """Calculate comprehensive upsize metrics with revenue tracking"""
        total_opportunities = 0
        total_offers = 0
        total_successes = 0
        largest_offers = 0
        total_revenue = 0.0
        
        # Item-specific tracking
        item_opportunities = defaultdict(int)
        item_offers = defaultdict(int)
        item_successes = defaultdict(int)
        item_revenue = defaultdict(float)
        
        # Upsize type tracking
        upsize_items_counter = Counter()
        
        for transaction in transactions:
            # Parse items initially requested
            initial_items = UpsizeAnalytics._parse_items_field(transaction.get("Items Initially Requested", "0"))
            
            # Get upsize metrics
            opportunities = transaction.get("# of Chances to Upsize", 0)
            offers = transaction.get("# of Upsizing Offers Made", 0)
            successes = transaction.get("# of Sucessfull Upsizing chances", 0)
            largest = transaction.get("# of Times largest Option Offered", 0)
            
            total_opportunities += opportunities
            total_offers += offers
            total_successes += successes
            largest_offers += largest
            
            # Count successfully upsized items and calculate revenue
            upsized_items = UpsizeAnalytics._parse_items_field(transaction.get("Items Successfully Upsized", "0"))
            transaction_revenue = 0
            
            for item in upsized_items:
                upsize_items_counter[item] += 1
                
                # Calculate revenue if item_lookup is provided
                if item_lookup:
                    price = item_lookup.get_item_price(item)
                    total_revenue += price
                    transaction_revenue += price
            
            # Track by initial items and distribute revenue proportionally
            for item in initial_items:
                item_opportunities[item] += opportunities
                item_offers[item] += offers
                item_successes[item] += successes
                # Distribute transaction revenue among initial items
                if len(initial_items) > 0:
                    item_revenue[item] += transaction_revenue / len(initial_items)
        
        return {
            "total_opportunities": total_opportunities,
            "total_offers": total_offers,
            "total_successes": total_successes,
            "total_revenue": total_revenue,
            "avg_revenue_per_success": total_revenue / total_successes if total_successes > 0 else 0,
            "largest_offers": largest_offers,
            "success_rate": (total_successes / total_offers * 100) if total_offers > 0 else 0,
            "offer_rate": (total_offers / total_opportunities * 100) if total_opportunities > 0 else 0,
            "conversion_rate": (total_successes / total_opportunities * 100) if total_opportunities > 0 else 0,
            "largest_offer_rate": (largest_offers / total_offers * 100) if total_offers > 0 else 0,
            "by_item": {
                item: {
                    "opportunities": item_opportunities[item],
                    "offers": item_offers[item],
                    "successes": item_successes[item],
                    "revenue": item_revenue[item],
                    "success_rate": (item_successes[item] / item_offers[item] * 100) if item_offers[item] > 0 else 0,
                    "offer_rate": (item_offers[item] / item_opportunities[item] * 100) if item_opportunities[item] > 0 else 0
                }
                for item in set(list(item_opportunities.keys()) + list(item_offers.keys()) + list(item_successes.keys()))
            },
            "most_upsized_items": dict(upsize_items_counter.most_common(10))
        }
    
    @staticmethod
    def calculate_upsize_metrics_by_operator(transactions: List[Dict], item_lookup=None) -> Dict[str, Any]:
        """Calculate upsize metrics broken down by operator"""
        operator_metrics = defaultdict(lambda: {
            "total_opportunities": 0,
            "total_offers": 0,
            "total_successes": 0,
            "largest_offers": 0,
            "total_revenue": 0.0,
            "item_opportunities": defaultdict(int),
            "item_offers": defaultdict(int),
            "item_successes": defaultdict(int),
            "item_revenue": defaultdict(float),
            "upsize_items_counter": Counter()
        })
        
        for transaction in transactions:
            # Try both possible operator field names for compatibility
            operator = transaction.get("Operator Name", transaction.get("Operator", "Unknown Operator"))
            
            # Parse items initially requested
            initial_items = UpsizeAnalytics._parse_items_field(transaction.get("Items Initially Requested", "0"))
            
            # Get upsize metrics
            opportunities = transaction.get("# of Chances to Upsize", 0)
            offers = transaction.get("# of Upsizing Offers Made", 0)
            successes = transaction.get("# of Sucessfull Upsizing chances", 0)
            largest = transaction.get("# of Times largest Option Offered", 0)
            
            operator_data = operator_metrics[operator]
            operator_data["total_opportunities"] += opportunities
            operator_data["total_offers"] += offers
            operator_data["total_successes"] += successes
            operator_data["largest_offers"] += largest
            
            # Count successfully upsized items and calculate revenue
            upsized_items = UpsizeAnalytics._parse_items_field(transaction.get("Items Successfully Upsized", "0"))
            transaction_revenue = 0
            
            for item in upsized_items:
                operator_data["upsize_items_counter"][item] += 1
                
                # Calculate revenue if item_lookup is provided
                if item_lookup:
                    price = item_lookup.get_item_price(item)
                    operator_data["total_revenue"] += price
                    transaction_revenue += price
            
            # Track by initial items and distribute revenue proportionally
            for item in initial_items:
                operator_data["item_opportunities"][item] += opportunities
                operator_data["item_offers"][item] += offers
                operator_data["item_successes"][item] += successes
                # Distribute transaction revenue among initial items
                if len(initial_items) > 0:
                    operator_data["item_revenue"][item] += transaction_revenue / len(initial_items)
        
        # Convert to final format
        result = {}
        for operator, data in operator_metrics.items():
            result[operator] = {
                "total_opportunities": data["total_opportunities"],
                "total_offers": data["total_offers"],
                "total_successes": data["total_successes"],
                "total_revenue": data["total_revenue"],
                "avg_revenue_per_success": data["total_revenue"] / data["total_successes"] if data["total_successes"] > 0 else 0,
                "largest_offers": data["largest_offers"],
                "success_rate": (data["total_successes"] / data["total_offers"] * 100) if data["total_offers"] > 0 else 0,
                "offer_rate": (data["total_offers"] / data["total_opportunities"] * 100) if data["total_opportunities"] > 0 else 0,
                "conversion_rate": (data["total_successes"] / data["total_opportunities"] * 100) if data["total_opportunities"] > 0 else 0,
                "largest_offer_rate": (data["largest_offers"] / data["total_offers"] * 100) if data["total_offers"] > 0 else 0,
                "by_item": {
                    item: {
                        "opportunities": data["item_opportunities"][item],
                        "offers": data["item_offers"][item],
                        "successes": data["item_successes"][item],
                        "revenue": data["item_revenue"][item],
                        "success_rate": (data["item_successes"][item] / data["item_offers"][item] * 100) if data["item_offers"][item] > 0 else 0,
                        "offer_rate": (data["item_offers"][item] / data["item_opportunities"][item] * 100) if data["item_opportunities"][item] > 0 else 0
                    }
                    for item in set(list(data["item_opportunities"].keys()) + list(data["item_offers"].keys()) + list(data["item_successes"].keys()))
                },
                "most_upsized_items": dict(data["upsize_items_counter"].most_common(10))
            }
        
        return result
    
    @staticmethod
    def _parse_items_field(items_field: Any) -> List[str]:
        """Parse items field which can be string, list, or dict"""
        return UpsellAnalytics._parse_items_field(items_field)


class AddonAnalytics:
    """Handles add-on analytics calculations"""
    
    @staticmethod
    def calculate_addon_metrics(transactions: List[Dict], item_lookup=None) -> Dict[str, Any]:
        """Calculate comprehensive add-on metrics with revenue tracking"""
        total_opportunities = 0
        total_offers = 0
        total_successes = 0
        total_revenue = 0.0
        
        # Item-specific tracking
        item_opportunities = defaultdict(int)
        item_offers = defaultdict(int)
        item_successes = defaultdict(int)
        item_revenue = defaultdict(float)
        
        # Add-on type tracking
        addon_items_counter = Counter()
        addon_types_counter = Counter()
        
        for transaction in transactions:
            # Parse items that could have add-ons (this is what we should track by)
            addon_capable_items = AddonAnalytics._parse_items_field(transaction.get("Items in Order that could have Add-Ons", "0"))
            
            # Get add-on metrics
            opportunities = transaction.get("# of Chances to Add-on", 0)
            offers = transaction.get("# of Add-on Offers", 0)
            successes = transaction.get("# of Succesful Add-on Offers", 0)
            
            total_opportunities += opportunities
            total_offers += offers
            total_successes += successes
            
            # Count successfully added items and calculate revenue
            addon_items = AddonAnalytics._parse_items_field(transaction.get("Items with Successful Add-Ons", "0"))
            transaction_revenue = 0
            
            for item in addon_items:
                addon_items_counter[item] += 1
                
                # Calculate revenue if item_lookup is provided
                if item_lookup:
                    price = item_lookup.get_item_price(item)
                    total_revenue += price
                    transaction_revenue += price
            
            # Track by items that could have add-ons and distribute revenue proportionally
            for item in addon_capable_items:
                item_opportunities[item] += opportunities
                item_offers[item] += offers
                item_successes[item] += successes
                addon_types_counter[item] += 1
                # Distribute transaction revenue among addon-capable items
                if len(addon_capable_items) > 0:
                    item_revenue[item] += transaction_revenue / len(addon_capable_items)
        
        return {
            "total_opportunities": total_opportunities,
            "total_offers": total_offers,
            "total_successes": total_successes,
            "total_revenue": total_revenue,
            "avg_revenue_per_success": total_revenue / total_successes if total_successes > 0 else 0,
            "success_rate": (total_successes / total_offers * 100) if total_offers > 0 else 0,
            "offer_rate": (total_offers / total_opportunities * 100) if total_opportunities > 0 else 0,
            "conversion_rate": (total_successes / total_opportunities * 100) if total_opportunities > 0 else 0,
            "by_item": {
                item: {
                    "opportunities": item_opportunities[item],
                    "offers": item_offers[item],
                    "successes": item_successes[item],
                    "revenue": item_revenue[item],
                    "success_rate": (item_successes[item] / item_offers[item] * 100) if item_offers[item] > 0 else 0,
                    "offer_rate": (item_offers[item] / item_opportunities[item] * 100) if item_opportunities[item] > 0 else 0
                }
                for item in set(list(item_opportunities.keys()) + list(item_offers.keys()) + list(item_successes.keys()))
            },
            "most_successful_addons": dict(addon_items_counter.most_common(10)),
            "most_offered_addon_types": dict(addon_types_counter.most_common(10))
        }
    
    @staticmethod
    def calculate_addon_metrics_by_operator(transactions: List[Dict], item_lookup=None) -> Dict[str, Any]:
        """Calculate add-on metrics broken down by operator"""
        operator_metrics = defaultdict(lambda: {
            "total_opportunities": 0,
            "total_offers": 0,
            "total_successes": 0,
            "total_revenue": 0.0,
            "item_opportunities": defaultdict(int),
            "item_offers": defaultdict(int),
            "item_successes": defaultdict(int),
            "item_revenue": defaultdict(float),
            "addon_items_counter": Counter(),
            "addon_types_counter": Counter()
        })
        
        for transaction in transactions:
            # Try both possible operator field names for compatibility
            operator = transaction.get("Operator Name", transaction.get("Operator", "Unknown Operator"))
            
            # Parse items that could have add-ons (this is what we should track by)
            addon_capable_items = AddonAnalytics._parse_items_field(transaction.get("Items in Order that could have Add-Ons", "0"))
            
            # Get add-on metrics
            opportunities = transaction.get("# of Chances to Add-on", 0)
            offers = transaction.get("# of Add-on Offers", 0)
            successes = transaction.get("# of Succesful Add-on Offers", 0)
            
            operator_data = operator_metrics[operator]
            operator_data["total_opportunities"] += opportunities
            operator_data["total_offers"] += offers
            operator_data["total_successes"] += successes
            
            # Count successfully added items and calculate revenue
            addon_items = AddonAnalytics._parse_items_field(transaction.get("Items with Successful Add-Ons", "0"))
            transaction_revenue = 0
            
            for item in addon_items:
                operator_data["addon_items_counter"][item] += 1
                
                # Calculate revenue if item_lookup is provided
                if item_lookup:
                    price = item_lookup.get_item_price(item)
                    operator_data["total_revenue"] += price
                    transaction_revenue += price
            
            # Track by items that could have add-ons and distribute revenue proportionally
            for item in addon_capable_items:
                operator_data["item_opportunities"][item] += opportunities
                operator_data["item_offers"][item] += offers
                operator_data["item_successes"][item] += successes
                operator_data["addon_types_counter"][item] += 1
                # Distribute transaction revenue among addon-capable items
                if len(addon_capable_items) > 0:
                    operator_data["item_revenue"][item] += transaction_revenue / len(addon_capable_items)
        
        # Convert to final format
        result = {}
        for operator, data in operator_metrics.items():
            result[operator] = {
                "total_opportunities": data["total_opportunities"],
                "total_offers": data["total_offers"],
                "total_successes": data["total_successes"],
                "total_revenue": data["total_revenue"],
                "avg_revenue_per_success": data["total_revenue"] / data["total_successes"] if data["total_successes"] > 0 else 0,
                "success_rate": (data["total_successes"] / data["total_offers"] * 100) if data["total_offers"] > 0 else 0,
                "offer_rate": (data["total_offers"] / data["total_opportunities"] * 100) if data["total_opportunities"] > 0 else 0,
                "conversion_rate": (data["total_successes"] / data["total_opportunities"] * 100) if data["total_opportunities"] > 0 else 0,
                "by_item": {
                    item: {
                        "opportunities": data["item_opportunities"][item],
                        "offers": data["item_offers"][item],
                        "successes": data["item_successes"][item],
                        "revenue": data["item_revenue"][item],
                        "success_rate": (data["item_successes"][item] / data["item_offers"][item] * 100) if data["item_offers"][item] > 0 else 0,
                        "offer_rate": (data["item_offers"][item] / data["item_opportunities"][item] * 100) if data["item_opportunities"][item] > 0 else 0
                    }
                    for item in set(list(data["item_opportunities"].keys()) + list(data["item_offers"].keys()) + list(data["item_successes"].keys()))
                },
                "most_successful_addons": dict(data["addon_items_counter"].most_common(10)),
                "most_offered_addon_types": dict(data["addon_types_counter"].most_common(10))
            }
        
        return result
    
    @staticmethod
    def _parse_items_field(items_field: Any) -> List[str]:
        """Parse items field which can be string, list, or dict"""
        return UpsellAnalytics._parse_items_field(items_field)


class HoptixAnalyticsService:
    """Main analytics service for Hoptix grading data - now using graded_rows_filtered view"""
    
    def __init__(self, db: Supa):
        self.db = db
        self.upsell_analytics = UpsellAnalytics()
        self.upsize_analytics = UpsizeAnalytics()
        self.addon_analytics = AddonAnalytics()
        self.item_lookup = get_item_lookup_service()
    
    def get_run_transactions(self, run_id: str) -> List[Dict[str, Any]]:
        """Get all transactions for a run from the graded_rows_filtered view"""
        try:
            result = self.db.client.from_('graded_rows_filtered')\
                .select('*')\
                .eq('Run ID', run_id)\
                .execute()
            
            return result.data if result.data else []
            
        except Exception as e:
            logger.error(f"Error retrieving transactions for run {run_id}: {e}")
            return []
    
    def get_location_transactions(self, location_id: str, days: int = 30) -> List[Dict[str, Any]]:
        """Get transactions for a location from the graded_rows_filtered view"""
        try:
            # Note: You'll need to add location_id to the graded_rows_filtered view
            # For now, we'll use a different approach via runs table
            runs_result = self.db.client.table('runs')\
                .select('id')\
                .eq('location_id', location_id)\
                .gte('run_date', f'now() - interval \'{days} days\'')\
                .execute()
            
            if not runs_result.data:
                return []
            
            run_ids = [run['id'] for run in runs_result.data]
            
            # Get transactions for these runs
            result = self.db.client.from_('graded_rows_filtered')\
                .select('*')\
                .in_('Run ID', run_ids)\
                .execute()
            
            return result.data if result.data else []
            
        except Exception as e:
            logger.error(f"Error retrieving transactions for location {location_id}: {e}")
            return []
    
    def generate_run_report(self, run_id: str) -> Dict[str, Any]:
        """Generate a comprehensive analytics report for a specific run"""
        transactions = self.get_run_transactions(run_id)
        
        if not transactions:
            logger.warning(f"No transactions found for run {run_id}")
            return {}
        
        return self.generate_comprehensive_report(transactions)
    
    def generate_location_report(self, location_id: str, days: int = 30) -> Dict[str, Any]:
        """Generate a comprehensive analytics report for a location"""
        transactions = self.get_location_transactions(location_id, days)
        
        if not transactions:
            logger.warning(f"No transactions found for location {location_id}")
            return {}
        
        return self.generate_comprehensive_report(transactions)
    
    def get_operator_performance_by_run(self, run_id: str) -> List[Dict[str, Any]]:
        """Get operator performance data for a specific run"""
        transactions = self.get_run_transactions(run_id)
        
        if not transactions:
            return []
        
        # Group by operator
        operator_transactions = defaultdict(list)
        for transaction in transactions:
            operator = transaction.get("Operator Name", "Unknown Operator")
            operator_transactions[operator].append(transaction)
        
        # Calculate metrics for each operator
        operator_performance = []
        for operator, op_transactions in operator_transactions.items():
            # Generate mini-report for this operator
            operator_report = self.generate_comprehensive_report(op_transactions)
            
            # Extract key metrics
            performance = {
                "operator_name": operator,
                "transaction_count": len(op_transactions),
                "completion_rate": operator_report["summary"]["completion_rate"],
                "avg_items_increase": operator_report["summary"]["avg_item_increase"],
                
                # Upselling metrics
                "upsell_opportunities": operator_report["upselling"]["total_opportunities"],
                "upsell_offers": operator_report["upselling"]["total_offers"],
                "upsell_successes": operator_report["upselling"]["total_successes"],
                "upsell_conversion_rate": operator_report["upselling"]["conversion_rate"],
                "upsell_revenue": operator_report["upselling"]["total_revenue"],
                
                # Upsizing metrics
                "upsize_opportunities": operator_report["upsizing"]["total_opportunities"],
                "upsize_offers": operator_report["upsizing"]["total_offers"],
                "upsize_successes": operator_report["upsizing"]["total_successes"],
                "upsize_conversion_rate": operator_report["upsizing"]["conversion_rate"],
                "upsize_revenue": operator_report["upsizing"]["total_revenue"],
                
                # Add-on metrics
                "addon_opportunities": operator_report["addons"]["total_opportunities"],
                "addon_offers": operator_report["addons"]["total_offers"],
                "addon_successes": operator_report["addons"]["total_successes"],
                "addon_conversion_rate": operator_report["addons"]["conversion_rate"],
                "addon_revenue": operator_report["addons"]["total_revenue"],
                
                # Overall metrics
                "total_opportunities": (
                    operator_report["upselling"]["total_opportunities"] +
                    operator_report["upsizing"]["total_opportunities"] +
                    operator_report["addons"]["total_opportunities"]
                ),
                "total_successes": (
                    operator_report["upselling"]["total_successes"] +
                    operator_report["upsizing"]["total_successes"] +
                    operator_report["addons"]["total_successes"]
                ),
                "total_revenue": (
                    operator_report["upselling"]["total_revenue"] +
                    operator_report["upsizing"]["total_revenue"] +
                    operator_report["addons"]["total_revenue"]
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
    
    def generate_comprehensive_report(self, transactions: List[Dict]) -> Dict[str, Any]:
        """Generate a comprehensive analytics report from transactions"""
        logger.info(f"Generating analytics report for {len(transactions)} transactions")
        
        # Calculate individual metrics
        upsell_metrics = self.upsell_analytics.calculate_upsell_metrics(transactions, self.item_lookup)
        upsize_metrics = self.upsize_analytics.calculate_upsize_metrics(transactions, self.item_lookup)
        addon_metrics = self.addon_analytics.calculate_addon_metrics(transactions, self.item_lookup)
        
        # Calculate operator-level metrics
        upsell_by_operator = self.upsell_analytics.calculate_upsell_metrics_by_operator(transactions, self.item_lookup)
        upsize_by_operator = self.upsize_analytics.calculate_upsize_metrics_by_operator(transactions, self.item_lookup)
        addon_by_operator = self.addon_analytics.calculate_addon_metrics_by_operator(transactions, self.item_lookup)
        
        # Calculate overall performance
        total_transactions = len(transactions)
        complete_transactions = sum(1 for t in transactions if t.get("Complete Transcript?", 0) == 1)
        
        # Calculate average items per transaction
        total_items_initial = sum(t.get("# of Items Ordered", 0) for t in transactions)
        total_items_final = sum(t.get("# of Items Ordered After Upselling, Upsizing, and Add-on Offers", 0) for t in transactions)
        
        # Top performing items analysis
        top_items = self._analyze_top_performing_items(transactions)
        
        # Time-based analysis if dates are available
        time_analysis = self._analyze_by_time_period(transactions)
        
        report = {
            "summary": {
                "total_transactions": total_transactions,
                "complete_transactions": complete_transactions,
                "completion_rate": (complete_transactions / total_transactions * 100) if total_transactions > 0 else 0,
                "avg_items_initial": total_items_initial / total_transactions if total_transactions > 0 else 0,
                "avg_items_final": total_items_final / total_transactions if total_transactions > 0 else 0,
                "avg_item_increase": (total_items_final - total_items_initial) / total_transactions if total_transactions > 0 else 0,
                "generated_at": datetime.now().isoformat()
            },
            "upselling": upsell_metrics,
            "upsizing": upsize_metrics,
            "addons": addon_metrics,
            "operator_analytics": {
                "upselling": upsell_by_operator,
                "upsizing": upsize_by_operator,
                "addons": addon_by_operator
            },
            "top_performing_items": top_items,
            "time_analysis": time_analysis,
            "recommendations": self._generate_recommendations(upsell_metrics, upsize_metrics, addon_metrics)
        }
        
        # Enhance report with actual item names
        enhanced_report = self.item_lookup.enhance_analytics_data(report)
        
        logger.info("Analytics report generated successfully")
        return enhanced_report
    
    def _analyze_top_performing_items(self, transactions: List[Dict]) -> Dict[str, Any]:
        """Analyze top performing items across all categories"""
        item_performance = defaultdict(lambda: {
            "total_opportunities": 0,
            "total_offers": 0,
            "total_successes": 0,
            "upsell_successes": 0,
            "upsize_successes": 0,
            "addon_successes": 0,
            "frequency": 0
        })
        
        for transaction in transactions:
            initial_items = UpsellAnalytics._parse_items_field(transaction.get("Items Initially Requested", "0"))
            
            for item in initial_items:
                stats = item_performance[item]
                stats["frequency"] += 1
                stats["total_opportunities"] += (
                    transaction.get("# of Chances to Upsell", 0) +
                    transaction.get("# of Chances to Upsize", 0) +
                    transaction.get("# of Chances to Add-on", 0)
                )
                stats["total_offers"] += (
                    transaction.get("# of Upselling Offers Made", 0) +
                    transaction.get("# of Upsizing Offers Made", 0) +
                    transaction.get("# of Add-on Offers", 0)
                )
                stats["total_successes"] += (
                    transaction.get("# of Sucessfull Upselling chances", 0) +
                    transaction.get("# of Sucessfull Upsizing chances", 0) +
                    transaction.get("# of Succesful Add-on Offers", 0)
                )
                stats["upsell_successes"] += transaction.get("# of Sucessfull Upselling chances", 0)
                stats["upsize_successes"] += transaction.get("# of Sucessfull Upsizing chances", 0)
                stats["addon_successes"] += transaction.get("# of Succesful Add-on Offers", 0)
        
        # Calculate performance rates and sort
        for item, stats in item_performance.items():
            stats["success_rate"] = (stats["total_successes"] / stats["total_offers"] * 100) if stats["total_offers"] > 0 else 0
            stats["offer_rate"] = (stats["total_offers"] / stats["total_opportunities"] * 100) if stats["total_opportunities"] > 0 else 0
        
        # Sort by different criteria
        by_frequency = sorted(item_performance.items(), key=lambda x: x[1]["frequency"], reverse=True)[:10]
        by_success_rate = sorted(item_performance.items(), key=lambda x: x[1]["success_rate"], reverse=True)[:10]
        by_total_successes = sorted(item_performance.items(), key=lambda x: x[1]["total_successes"], reverse=True)[:10]
        
        return {
            "most_frequent_items": {item: stats for item, stats in by_frequency},
            "highest_success_rate_items": {item: stats for item, stats in by_success_rate},
            "most_successful_items": {item: stats for item, stats in by_total_successes}
        }
    
    def _analyze_by_time_period(self, transactions: List[Dict]) -> Dict[str, Any]:
        """Analyze performance by time periods"""
        time_buckets = defaultdict(lambda: {
            "transactions": 0,
            "upsell_successes": 0,
            "upsize_successes": 0,
            "addon_successes": 0,
            "total_opportunities": 0
        })
        
        for transaction in transactions:
            # Extract date if available
            date_str = transaction.get("Date", "")
            if date_str:
                try:
                    # Parse date and create time bucket (by day)
                    date_obj = datetime.strptime(date_str, "%m/%d/%Y")
                    time_key = date_obj.strftime("%Y-%m-%d")
                    
                    bucket = time_buckets[time_key]
                    bucket["transactions"] += 1
                    bucket["upsell_successes"] += transaction.get("# of Sucessfull Upselling chances", 0)
                    bucket["upsize_successes"] += transaction.get("# of Sucessfull Upsizing chances", 0)
                    bucket["addon_successes"] += transaction.get("# of Succesful Add-on Offers", 0)
                    bucket["total_opportunities"] += (
                        transaction.get("# of Chances to Upsell", 0) +
                        transaction.get("# of Chances to Upsize", 0) +
                        transaction.get("# of Chances to Add-on", 0)
                    )
                except ValueError:
                    # Skip if date parsing fails
                    continue
        
        # Calculate rates for each time period
        for time_key, bucket in time_buckets.items():
            total_successes = bucket["upsell_successes"] + bucket["upsize_successes"] + bucket["addon_successes"]
            bucket["success_rate"] = (total_successes / bucket["total_opportunities"] * 100) if bucket["total_opportunities"] > 0 else 0
        
        return dict(time_buckets)
    
    def _generate_recommendations(self, upsell_metrics: Dict, upsize_metrics: Dict, addon_metrics: Dict) -> List[str]:
        """Generate actionable recommendations based on analytics"""
        recommendations = []
        
        # Upselling recommendations
        if upsell_metrics["offer_rate"] < 50:
            recommendations.append(f"🎯 Upselling offer rate is only {upsell_metrics['offer_rate']:.1f}%. Train staff to identify and act on more upselling opportunities.")
        
        if upsell_metrics["success_rate"] < 30:
            recommendations.append(f"📈 Upselling success rate is {upsell_metrics['success_rate']:.1f}%. Review upselling scripts and techniques to improve conversion.")
        
        # Upsizing recommendations
        if upsize_metrics["offer_rate"] < 60:
            recommendations.append(f"📏 Upsizing offer rate is {upsize_metrics['offer_rate']:.1f}%. Encourage staff to suggest larger sizes more consistently.")
        
        if upsize_metrics["largest_offer_rate"] < 80:
            recommendations.append(f"⬆️ Only {upsize_metrics['largest_offer_rate']:.1f}% of upsize offers mention the largest option. Train staff to always offer the premium size.")
        
        # Add-on recommendations
        if addon_metrics["offer_rate"] < 40:
            recommendations.append(f"🍟 Add-on offer rate is {addon_metrics['offer_rate']:.1f}%. Focus on suggesting extras like toppings, sides, and premium options.")
        
        # Top item recommendations
        if upsell_metrics.get("most_upsold_items"):
            top_upsold = list(upsell_metrics["most_upsold_items"].keys())[0]
            recommendations.append(f"⭐ '{top_upsold}' is your top upsold item. Create targeted promotions around this success.")
        
        if not recommendations:
            recommendations.append("🎉 Great performance across all metrics! Continue current training and focus on consistency.")
        
        return recommendations

    def get_item_specific_report(self, transactions: List[Dict], item_filter: Optional[str] = None) -> Dict[str, Any]:
        """Generate a report focused on specific items"""
        if item_filter:
            # Filter transactions that include the specific item
            filtered_transactions = []
            for transaction in transactions:
                initial_items = UpsellAnalytics._parse_items_field(transaction.get("Items Initially Requested", "0"))
                if any(item_filter.lower() in str(item).lower() for item in initial_items):
                    filtered_transactions.append(transaction)
            transactions = filtered_transactions
        
        return self.generate_comprehensive_report(transactions)
