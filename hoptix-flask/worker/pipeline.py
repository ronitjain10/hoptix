import os, time, tempfile, json, datetime as dt, logging
from typing import List, Dict, Tuple
from dateutil import parser as dateparse
from integrations.db_supabase import Supa
from integrations.s3_client import get_s3, download_to_file, put_jsonl
from worker.adapter import transcribe_video, split_into_transactions, grade_transactions

# Configure logging for pipeline
logger = logging.getLogger(__name__)

from config import Settings


def fetch_one_uploaded_video(db: Supa):
    r = db.client.table("videos").select(
        "id, s3_key, run_id, location_id, started_at, ended_at"
    ).eq("status","uploaded").limit(1).execute()
    return r.data[0] if r.data else None

def claim_video(db: Supa, video_id: str) -> bool:
    r = db.client.table("videos").update({"status":"processing"}) \
        .eq("id", video_id).eq("status","uploaded").execute()
    return bool(r.data)

def mark_status(db: Supa, video_id: str, status: str):
    db.client.table("videos").update({"status":status}).eq("id",video_id).execute()

def insert_transactions(db: Supa, video_row: Dict, transactions: List[Dict]) -> List[str]:
    logger.debug(f"Preparing {len(transactions)} transactions for insertion")
    
    # Handle empty transactions list
    if not transactions:
        logger.warning("No transactions to insert - empty list provided")
        return []
    
    # Log timing information for verification
    for i, tx in enumerate(transactions):
        meta = tx.get("meta", {})
        logger.info(f"Transaction {i+1}: {tx['started_at']} to {tx['ended_at']} "
                   f"(video seconds {meta.get('video_start_seconds', 'N/A')}-{meta.get('video_end_seconds', 'N/A')})")
    
    rows = []
    for tx in transactions:
        rows.append({
            "video_id": video_row["id"],
            "run_id":   video_row["run_id"],
            "started_at": tx["started_at"],
            "ended_at":   tx["ended_at"],
            "tx_range":   f'["{tx["started_at"]}","{tx["ended_at"]}")',
            "kind":       tx.get("kind"),
            "meta":       tx.get("meta", {})
        })
    
    try:
        # Insert transactions - Supabase should return full records by default
        logger.debug(f"Inserting {len(rows)} transaction rows")
        ins = db.client.table("transactions").insert(rows).execute()
        
        if not ins.data:
            logger.error("Failed to insert transactions - no data returned")
            return []
        
        # The insert should return the full records including IDs
        tx_ids = [r["id"] for r in ins.data]
        logger.debug(f"Successfully inserted {len(tx_ids)} transactions")
        return tx_ids
        
    except Exception as e:
        logger.error(f"Error inserting transactions: {str(e)}")
        logger.error(f"Transaction data sample: {rows[0] if rows else 'No rows'}")
        raise



def upsert_grades(db: Supa, tx_ids: List[str], grades: List[Dict]):
    logger.debug(f"Preparing grades for {len(tx_ids)} transactions")
    
    # Handle empty lists
    if not tx_ids or not grades:
        logger.warning("No grades to upsert - empty transaction IDs or grades list")
        return
    
    grads = []
    for tx_id, g in zip(tx_ids, grades):
        d = g.get("details", {}) or {}
        grads.append({
            "transaction_id": tx_id,

            # compatibility booleans + score
            "upsell_possible": g.get("upsell_possible"),
            "upsell_offered":  g.get("upsell_offered"),
            "upsize_possible": g.get("upsize_possible"),
            "upsize_offered":  g.get("upsize_offered"),
            "score":           g.get("score"),

            # FULL column set from your request
            "items_initial": d.get("items_initial"),
            "num_items_initial": d.get("num_items_initial"),
            "num_upsell_opportunities": d.get("num_upsell_opportunities"),
            "items_upsellable": d.get("items_upsellable"),
            "num_upsell_offers": d.get("num_upsell_offers"),
            "items_upsold": d.get("items_upsold"),
            "num_upsell_success": d.get("num_upsell_success"),
            "num_largest_offers": d.get("num_largest_offers"),
            "num_upsize_opportunities": d.get("num_upsize_opportunities"),
            "items_upsizeable": d.get("items_upsizeable"),
            "num_upsize_offers": d.get("num_upsize_offers"),
            "num_upsize_success": d.get("num_upsize_success"),
            "items_upsize_success": d.get("items_upsize_success"),
            "num_addon_opportunities": d.get("num_addon_opportunities"),
            "items_addonable": d.get("items_addonable"),
            "num_addon_offers": d.get("num_addon_offers"),
            "num_addon_success": d.get("num_addon_success"),
            "items_addon_success": d.get("items_addon_success"),
            "items_after": d.get("items_after"),
            "num_items_after": d.get("num_items_after"),
            "feedback": d.get("feedback"),
            "issues": d.get("issues"),

            "complete_order": d.get("complete_order"),
            "mobile_order": d.get("mobile_order"),
            "coupon_used": d.get("coupon_used"),
            "asked_more_time": d.get("asked_more_time"),
            "out_of_stock_items": d.get("out_of_stock_items"),
            "gpt_price": g.get("gpt_price", 0),  # Get from grade object, not details
            "reasoning_summary": d.get("reasoning_summary"),
            "video_file_path": d.get("video_file_path"),
            "video_link": d.get("video_link"),
            
            # New fields for updated table
            "transcript": g.get("transcript", ""),  # Raw transcript

            # always keep a full blob too
            "details": d
        })
    
    if grads:
        try:
            #db.client.table("grades").upsert(grads, on_conflict="transaction_id").execute()
            db.client.table("dev_grades").upsert(grads, on_conflict="transaction_id").execute()
            logger.debug(f"Successfully upserted {len(grads)} grades")
        except Exception as e:
            logger.error(f"Error upserting grades: {str(e)}")
            raise
    else:
        logger.warning("No grades to upsert")

def process_one_video(db: Supa, s3, video_row: Dict):
    video_id = video_row["id"]
    s3_key = video_row["s3_key"]
    logger.info(f"Starting video processing pipeline for {video_id}")
    
    settings = Settings()
    tmpdir = tempfile.mkdtemp(prefix="hoptix_")
    local_path = os.path.join(tmpdir, "input.mp4")
    
    try:
        # Download video
        logger.info(f"Downloading video {video_id} from S3: {s3_key}")
        download_to_file(s3, settings.RAW_BUCKET, s3_key, local_path)
        logger.info(f"Downloaded video to: {local_path}")

        # 1) ASR segments
        logger.info(f"Starting transcription for video {video_id}")
        segments = transcribe_video(local_path)
        logger.info(f"Transcription completed: {len(segments)} segments generated")

        # 2) Step‑1 split
        logger.info(f"Starting transaction splitting for video {video_id}")
        txs = split_into_transactions(segments, video_row["started_at"], s3_key)
        logger.info(f"Transaction splitting completed: {len(txs)} transactions identified")

        # artifacts
        prefix = f'deriv/session={video_id}/'
        logger.info(f"Uploading artifacts to S3 with prefix: {prefix}")
        put_jsonl(s3, settings.DERIV_BUCKET, prefix + "segments.jsonl", segments)
        put_jsonl(s3, settings.DERIV_BUCKET, prefix + "transactions.jsonl", txs)
        logger.info("Artifacts uploaded to S3")

        # 3) persist transactions
        logger.info(f"Inserting {len(txs)} transactions into database")
        tx_ids = insert_transactions(db, video_row, txs)
        logger.info(f"Inserted transactions with IDs: {tx_ids}")

        # 4) step‑2 grading
        logger.info(f"Starting grading for {len(txs)} transactions")
        grades = grade_transactions(txs)
        put_jsonl(s3, settings.DERIV_BUCKET, prefix + "grades.jsonl", grades)
        logger.info("Grading completed and uploaded to S3")

        # 5) upsert grades
        logger.info(f"Upserting grades for {len(tx_ids)} transactions")
        upsert_grades(db, tx_ids, grades)
        logger.info("Grades upsertion completed")
        
        logger.info(f"Successfully completed all processing steps for video {video_id}")
        
    finally:
        # Cleanup temporary files
        try:
            if os.path.exists(local_path):
                os.remove(local_path)
            if os.path.exists(tmpdir):
                os.rmdir(tmpdir)
            logger.info(f"Cleaned up temporary files for video {video_id}")
        except Exception as cleanup_error:
            logger.warning(f"Failed to cleanup temporary files: {cleanup_error}")

def main_loop():
    s = Settings()
    db = Supa(s.SUPABASE_URL, s.SUPABASE_SERVICE_KEY)
    s3 = get_s3(s.AWS_REGION)

    while True:
        row = fetch_one_uploaded_video(db)
        if not row:
            time.sleep(3); continue
        vid = row["id"]
        if not claim_video(db, vid):
            continue
        try:
            process_one_video(db, s3, row)
            mark_status(db, vid, "ready")
        except Exception as e:
            print("worker error:", e)
            mark_status(db, vid, "failed")
            time.sleep(2)