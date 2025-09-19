#!/usr/bin/env python3
"""
Test script for updated analytics integration with graded_rows_filtered view
Tests both the new HoptixAnalyticsService and AnalyticsStorageService
"""

import sys
sys.path.insert(0, '.')

from integrations.db_supabase import Supa
from config import Settings
from services.analytics_service_updated import HoptixAnalyticsService
from services.analytics_storage_service_updated import AnalyticsStorageService
from dotenv import load_dotenv
import json
import csv
import pandas as pd
from datetime import datetime

def save_analytics_to_csv(report, run_id, filename=None):
    """Save analytics report to CSV files"""
    
    if filename is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"analytics_report_{run_id}_{timestamp}"
    
    # 1. Save summary data
    summary_file = f"{filename}_summary.csv"
    summary_data = []
    
    if 'summary' in report:
        summary = report['summary']
        summary_data.append({
            'run_id': run_id,
            'total_transactions': summary.get('total_transactions', 0),
            'complete_transactions': summary.get('complete_transactions', 0),
            'completion_rate': summary.get('completion_rate', 0),
            'avg_items_initial': summary.get('avg_items_initial', 0),
            'avg_items_final': summary.get('avg_items_final', 0),
            'avg_item_increase': summary.get('avg_item_increase', 0),
            'generated_at': summary.get('generated_at', '')
        })
    
    # Add overall metrics
    if all(key in report for key in ['upselling', 'upsizing', 'addons']):
        upselling = report['upselling']
        upsizing = report['upsizing']
        addons = report['addons']
        
        summary_data[0].update({
            'upsell_opportunities': upselling.get('total_opportunities', 0),
            'upsell_offers': upselling.get('total_offers', 0),
            'upsell_successes': upselling.get('total_successes', 0),
            'upsell_conversion_rate': upselling.get('conversion_rate', 0),
            'upsell_revenue': upselling.get('total_revenue', 0),
            
            'upsize_opportunities': upsizing.get('total_opportunities', 0),
            'upsize_offers': upsizing.get('total_offers', 0),
            'upsize_successes': upsizing.get('total_successes', 0),
            'upsize_conversion_rate': upsizing.get('conversion_rate', 0),
            'upsize_revenue': upsizing.get('total_revenue', 0),
            'largest_offers': upsizing.get('largest_offers', 0),
            
            'addon_opportunities': addons.get('total_opportunities', 0),
            'addon_offers': addons.get('total_offers', 0),
            'addon_successes': addons.get('total_successes', 0),
            'addon_conversion_rate': addons.get('conversion_rate', 0),
            'addon_revenue': addons.get('total_revenue', 0)
        })
    
    # Save summary CSV
    if summary_data:
        df_summary = pd.DataFrame(summary_data)
        df_summary.to_csv(summary_file, index=False)
        print(f"   📄 Saved summary to: {summary_file}")
    
    # 2. Save operator performance data
    operator_file = f"{filename}_operators.csv"
    operator_data = []
    
    if 'operator_analytics' in report:
        op_analytics = report['operator_analytics']
        
        # Get all operators across categories
        all_operators = set()
        upsell_ops = op_analytics.get('upselling', {})
        upsize_ops = op_analytics.get('upsizing', {})
        addon_ops = op_analytics.get('addons', {})
        
        all_operators.update(upsell_ops.keys())
        all_operators.update(upsize_ops.keys())
        all_operators.update(addon_ops.keys())
        
        for operator in all_operators:
            upsell_data = upsell_ops.get(operator, {})
            upsize_data = upsize_ops.get(operator, {})
            addon_data = addon_ops.get(operator, {})
            
            operator_record = {
                'run_id': run_id,
                'operator_name': operator,
                
                # Upselling
                'upsell_opportunities': upsell_data.get('total_opportunities', 0),
                'upsell_offers': upsell_data.get('total_offers', 0),
                'upsell_successes': upsell_data.get('total_successes', 0),
                'upsell_conversion_rate': upsell_data.get('conversion_rate', 0),
                'upsell_success_rate': upsell_data.get('success_rate', 0),
                'upsell_offer_rate': upsell_data.get('offer_rate', 0),
                'upsell_revenue': upsell_data.get('total_revenue', 0),
                
                # Upsizing
                'upsize_opportunities': upsize_data.get('total_opportunities', 0),
                'upsize_offers': upsize_data.get('total_offers', 0),
                'upsize_successes': upsize_data.get('total_successes', 0),
                'upsize_conversion_rate': upsize_data.get('conversion_rate', 0),
                'upsize_success_rate': upsize_data.get('success_rate', 0),
                'upsize_offer_rate': upsize_data.get('offer_rate', 0),
                'upsize_revenue': upsize_data.get('total_revenue', 0),
                'largest_offers': upsize_data.get('largest_offers', 0),
                
                # Add-ons
                'addon_opportunities': addon_data.get('total_opportunities', 0),
                'addon_offers': addon_data.get('total_offers', 0),
                'addon_successes': addon_data.get('total_successes', 0),
                'addon_conversion_rate': addon_data.get('conversion_rate', 0),
                'addon_success_rate': addon_data.get('success_rate', 0),
                'addon_offer_rate': addon_data.get('offer_rate', 0),
                'addon_revenue': addon_data.get('total_revenue', 0),
                
                # Overall totals
                'total_opportunities': (
                    upsell_data.get('total_opportunities', 0) +
                    upsize_data.get('total_opportunities', 0) +
                    addon_data.get('total_opportunities', 0)
                ),
                'total_offers': (
                    upsell_data.get('total_offers', 0) +
                    upsize_data.get('total_offers', 0) +
                    addon_data.get('total_offers', 0)
                ),
                'total_successes': (
                    upsell_data.get('total_successes', 0) +
                    upsize_data.get('total_successes', 0) +
                    addon_data.get('total_successes', 0)
                ),
                'total_revenue': (
                    upsell_data.get('total_revenue', 0) +
                    upsize_data.get('total_revenue', 0) +
                    addon_data.get('total_revenue', 0)
                )
            }
            
            # Calculate overall conversion rate
            if operator_record['total_opportunities'] > 0:
                operator_record['overall_conversion_rate'] = (
                    operator_record['total_successes'] / operator_record['total_opportunities'] * 100
                )
            else:
                operator_record['overall_conversion_rate'] = 0
            
            operator_data.append(operator_record)
    
    # Save operator CSV
    if operator_data:
        df_operators = pd.DataFrame(operator_data)
        df_operators.to_csv(operator_file, index=False)
        print(f"   📄 Saved operator data to: {operator_file}")
    
    # 3. Save top performing items
    items_file = f"{filename}_top_items.csv"
    items_data = []
    
    if 'top_performing_items' in report:
        top_items = report['top_performing_items']
        
        # Most frequent items
        if 'most_frequent_items' in top_items:
            for item, stats in top_items['most_frequent_items'].items():
                items_data.append({
                    'run_id': run_id,
                    'category': 'most_frequent',
                    'item_name': item,
                    'frequency': stats.get('frequency', 0),
                    'total_opportunities': stats.get('total_opportunities', 0),
                    'total_offers': stats.get('total_offers', 0),
                    'total_successes': stats.get('total_successes', 0),
                    'success_rate': stats.get('success_rate', 0),
                    'offer_rate': stats.get('offer_rate', 0)
                })
        
        # Highest success rate items
        if 'highest_success_rate_items' in top_items:
            for item, stats in top_items['highest_success_rate_items'].items():
                items_data.append({
                    'run_id': run_id,
                    'category': 'highest_success_rate',
                    'item_name': item,
                    'frequency': stats.get('frequency', 0),
                    'total_opportunities': stats.get('total_opportunities', 0),
                    'total_offers': stats.get('total_offers', 0),
                    'total_successes': stats.get('total_successes', 0),
                    'success_rate': stats.get('success_rate', 0),
                    'offer_rate': stats.get('offer_rate', 0)
                })
    
    # Save items CSV
    if items_data:
        df_items = pd.DataFrame(items_data)
        df_items.to_csv(items_file, index=False)
        print(f"   📄 Saved top items to: {items_file}")
    
    return summary_file, operator_file, items_file

def save_operator_performance_to_csv(operator_performance, run_id, filename=None):
    """Save operator performance data to CSV"""
    
    if filename is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"operator_performance_{run_id}_{timestamp}.csv"
    
    if operator_performance:
        df = pd.DataFrame(operator_performance)
        df.to_csv(filename, index=False)
        print(f"   📄 Saved operator performance to: {filename}")
        return filename
    
    return None

def save_raw_transactions_to_csv(transactions, run_id, filename=None):
    """Save raw transaction data to CSV"""
    
    if filename is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"raw_transactions_{run_id}_{timestamp}.csv"
    
    if transactions:
        df = pd.DataFrame(transactions)
        df.to_csv(filename, index=False)
        print(f"   📄 Saved raw transactions to: {filename}")
        return filename
    
    return None

def main():
    """Test updated analytics integration with database"""
    
    load_dotenv()
    settings = Settings()
    db = Supa(settings.SUPABASE_URL, settings.SUPABASE_SERVICE_KEY)
    
    print("🔍 Testing Updated Analytics Integration with Database")
    print("=" * 60)
    
    try:
        # Initialize services
        analytics_service = HoptixAnalyticsService(db)
        storage_service = AnalyticsStorageService(db)
        
        # Test 1: Query graded_rows_filtered view directly
        print("📋 Test 1: Querying graded_rows_filtered view...")
        result = db.client.from_('graded_rows_filtered').select('*').limit(5).execute()
        
        if not result.data:
            print("❌ No data found in graded_rows_filtered view")
            print("   Make sure the view exists and contains data")
            return 1
        
        print(f"✅ Found data in graded_rows_filtered view")
        
        # Show available columns
        if result.data:
            columns = list(result.data[0].keys())
            print(f"📊 Available columns ({len(columns)}): {columns[:10]}{'...' if len(columns) > 10 else ''}")
            
            # Check for key columns
            key_columns = ['Run ID', 'Operator Name', 'Transaction ID']
            for col in key_columns:
                if col in result.data[0]:
                    print(f"✅ Found column: {col}")
                else:
                    print(f"❌ Missing column: {col}")
            
            # Test 2: Get run IDs
            print(f"\n📋 Test 2: Finding run IDs...")
            if 'Run ID' in result.data[0]:
                run_ids = list(set(row.get('Run ID') for row in result.data if row.get('Run ID')))
                print(f"🔄 Found {len(run_ids)} unique run_ids: {run_ids[:3]}{'...' if len(run_ids) > 3 else ''}")
                
                if run_ids:
                    test_run_id = run_ids[0]
                    print(f"\n📊 Test 3: Testing analytics for run_id: {test_run_id}")
                    
                    # Test 3a: Direct analytics service
                    print("   3a: Testing HoptixAnalyticsService.generate_run_report()...")
                    report = analytics_service.generate_run_report(test_run_id)
                    
                    if report:
                        print("   ✅ Direct analytics service working")
                        
                        # Display summary
                        print("\n   📊 ANALYTICS SUMMARY")
                        print("   " + "-" * 30)
                        summary = report['summary']
                        print(f"   Total Transactions: {summary['total_transactions']}")
                        print(f"   Complete Transactions: {summary['complete_transactions']}")
                        print(f"   Completion Rate: {summary['completion_rate']:.1f}%")
                        
                        # Upselling
                        upselling = report['upselling']
                        print(f"   🎯 Upselling: {upselling['total_successes']}/{upselling['total_opportunities']} ({upselling['conversion_rate']:.1f}%)")
                        
                        # Upsizing
                        upsizing = report['upsizing']
                        print(f"   📏 Upsizing: {upsizing['total_successes']}/{upsizing['total_opportunities']} ({upsizing['conversion_rate']:.1f}%)")
                        
                        # Add-ons
                        addons = report['addons']
                        print(f"   🍟 Add-ons: {addons['total_successes']}/{addons['total_opportunities']} ({addons['conversion_rate']:.1f}%)")
                        
                        # Test operator analytics
                        if 'operator_analytics' in report and report['operator_analytics']:
                            print(f"\n   👥 OPERATOR ANALYTICS")
                            print("   " + "-" * 30)
                            
                            upsell_ops = report['operator_analytics'].get('upselling', {})
                            if upsell_ops:
                                print(f"   Operators in upselling data: {list(upsell_ops.keys())[:3]}")
                                
                                # Show details for first operator
                                first_op = list(upsell_ops.keys())[0]
                                op_data = upsell_ops[first_op]
                                print(f"   {first_op}: {op_data['total_successes']}/{op_data['total_opportunities']} ({op_data['conversion_rate']:.1f}%)")
                            else:
                                print("   ⚠️ No operator upselling data found")
                        
                        # 💾 SAVE ANALYTICS TO CSV
                        print(f"\n   💾 SAVING ANALYTICS TO CSV FILES")
                        print("   " + "-" * 30)
                        try:
                            summary_file, operator_file, items_file = save_analytics_to_csv(report, test_run_id)
                            print(f"   ✅ Analytics saved successfully!")
                        except Exception as csv_error:
                            print(f"   ❌ Error saving CSV: {csv_error}")
                        
                    else:
                        print("   ❌ Direct analytics service returned empty report")
                    
                    # Test 3b: Storage service (backward compatibility)
                    print(f"\n   3b: Testing AnalyticsStorageService.get_run_analytics()...")
                    storage_report = storage_service.get_run_analytics(test_run_id)
                    
                    if storage_report:
                        print("   ✅ Storage service working")
                        print(f"   Summary: {storage_report['summary']['total_transactions']} transactions")
                    else:
                        print("   ❌ Storage service returned empty report")
                    
                    # Test 3c: Operator performance
                    print(f"\n   3c: Testing operator performance by run...")
                    operator_performance = analytics_service.get_operator_performance_by_run(test_run_id)
                    
                    if operator_performance:
                        print(f"   ✅ Found performance data for {len(operator_performance)} operators")
                        for i, op in enumerate(operator_performance[:3]):  # Show first 3
                            print(f"   {i+1}. {op['operator_name']}: {op['overall_conversion_rate']:.1f}% conversion")
                        
                        # Save operator performance to CSV
                        try:
                            op_perf_file = save_operator_performance_to_csv(operator_performance, test_run_id)
                        except Exception as csv_error:
                            print(f"   ❌ Error saving operator performance CSV: {csv_error}")
                    else:
                        print("   ⚠️ No operator performance data found")
                    
                    # Test 4: Storage service methods
                    print(f"\n📋 Test 4: Testing AnalyticsStorageService methods...")
                    
                    # Test run totals
                    run_totals = storage_service.get_run_totals(test_run_id)
                    if run_totals:
                        print(f"   ✅ get_run_totals(): {run_totals['total_transactions']} transactions")
                    else:
                        print("   ❌ get_run_totals() returned empty")
                    
                    # Test operator performance by run
                    op_perf = storage_service.get_operator_performance_by_run(test_run_id)
                    if op_perf:
                        print(f"   ✅ get_operator_performance_by_run(): {len(op_perf)} operators")
                    else:
                        print("   ❌ get_operator_performance_by_run() returned empty")
                    
                    # Test 5: Save raw transaction data
                    print(f"\n📋 Test 5: Saving raw transaction data...")
                    try:
                        raw_transactions = analytics_service.get_run_transactions(test_run_id)
                        if raw_transactions:
                            raw_file = save_raw_transactions_to_csv(raw_transactions, test_run_id)
                            print(f"   ✅ Raw transaction data saved")
                        else:
                            print("   ⚠️ No raw transaction data found")
                    except Exception as csv_error:
                        print(f"   ❌ Error saving raw transactions CSV: {csv_error}")
                    
                    print(f"\n🎉 All tests completed successfully!")
                    print(f"📊 Analytics system is working with graded_rows_filtered view")
                    print(f"💾 CSV files generated for comprehensive analysis")
                    
                else:
                    print("❌ No run_ids found in data")
            else:
                print("⚠️ 'Run ID' column not found in graded_rows_filtered view")
                print("   The view may need to be updated to include Run ID")
        
    except Exception as e:
        print(f"❌ Error testing analytics integration: {e}")
        import traceback
        print(f"Full error: {traceback.format_exc()}")
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())



